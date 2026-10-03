package k8s

import (
	"context"
	"fmt"
	"os"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Client wraps the k8s clientset.
type Client struct {
	cs        kubernetes.Interface
	namespace string
	image     string
}

// NewClient builds a k8s client.
// It prefers in-cluster config, falls back to KUBECONFIG / ~/.kube/config.
// K8S_SERVER_URL env can override the API server address (useful when the
// kubeconfig says 127.0.0.1 but we're inside a Docker container).
func NewClient(namespace, workerImage string) (*Client, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		// Fall back to kubeconfig (Docker Desktop / local dev)
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			kubeconfig = os.Getenv("HOME") + "/.kube/config"
		}
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("k8s config: %w", err)
		}
	}
	// Allow overriding the API server address (e.g. host.docker.internal from inside a container)
	if override := os.Getenv("K8S_SERVER_URL"); override != "" {
		cfg.Host = override
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("k8s clientset: %w", err)
	}

	// Ensure namespace exists.
	_, err = cs.CoreV1().Namespaces().Get(context.Background(), namespace, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		_, err = cs.CoreV1().Namespaces().Create(context.Background(), &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace},
		}, metav1.CreateOptions{})
	}
	if err != nil {
		return nil, fmt.Errorf("ensure namespace %s: %w", namespace, err)
	}

	return &Client{cs: cs, namespace: namespace, image: workerImage}, nil
}

// DeploymentSpec holds parameters for creating a worker Deployment.
type DeploymentSpec struct {
	Name         string
	JobID        string
	Replicas     int32
	MgmtDBDSN    string // reachable from inside k8s pods
	MgmtAPIURL   string // reachable from inside k8s pods
	KafkaBrokers string
}

func ptr[T any](v T) *T { return &v }

// CreateDeployment creates (or updates) a worker Deployment.
func (c *Client) CreateDeployment(ctx context.Context, spec DeploymentSpec) error {
	labels := map[string]string{
		"app":    "kafbat-worker",
		"job-id": spec.JobID,
	}

	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.Name,
			Namespace: c.namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr(spec.Replicas),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "worker",
							Image:           c.image,
							ImagePullPolicy: corev1.PullNever, // use local Docker Desktop image
							Env: []corev1.EnvVar{
								{Name: "JOB_ID", Value: spec.JobID},
								{Name: "MGMT_DB_DSN", Value: spec.MgmtDBDSN},
								{Name: "MGMT_API_URL", Value: spec.MgmtAPIURL},
								{Name: "KAFKA_BROKERS", Value: spec.KafkaBrokers},
							},
							Resources: corev1.ResourceRequirements{},
						},
					},
					RestartPolicy: corev1.RestartPolicyAlways,
				},
			},
		},
	}

	_, err := c.cs.AppsV1().Deployments(c.namespace).Create(ctx, d, metav1.CreateOptions{})
	if errors.IsAlreadyExists(err) {
		_, err = c.cs.AppsV1().Deployments(c.namespace).Update(ctx, d, metav1.UpdateOptions{})
	}
	return err
}

// ScaleDeployment sets replicas to 0 (pause) or back to n (resume).
func (c *Client) ScaleDeployment(ctx context.Context, name string, replicas int32) error {
	d, err := c.cs.AppsV1().Deployments(c.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	d.Spec.Replicas = ptr(replicas)
	_, err = c.cs.AppsV1().Deployments(c.namespace).Update(ctx, d, metav1.UpdateOptions{})
	return err
}

// GetDeploymentHealth checks whether a deployment is healthy and returns a
// (status, reason) pair. status is one of "running", "paused", "failed".
// reason is non-empty only when status is "failed".
func (c *Client) GetDeploymentHealth(ctx context.Context, deployName, jobID string) (status, reason string, err error) {
	d, getErr := c.cs.AppsV1().Deployments(c.namespace).Get(ctx, deployName, metav1.GetOptions{})
	if errors.IsNotFound(getErr) {
		return "failed", "k8s deployment not found", nil
	}
	if getErr != nil {
		return "", "", getErr
	}

	desired := int32(1)
	if d.Spec.Replicas != nil {
		desired = *d.Spec.Replicas
	}

	// Deployment was intentionally scaled to zero → paused.
	if desired == 0 {
		return "paused", "", nil
	}

	// All replicas unavailable → capture pod reason and report as paused-with-error
	// so the user can simply Resume rather than being forced to do a full offset reset.
	if d.Status.AvailableReplicas == 0 {
		reason := c.getFailedPodReason(ctx, jobID)
		if reason != "" {
			return "paused", reason, nil
		}
	}

	return "running", "", nil
}

// GetPodLogs returns the last `lines` log lines from the most-recently-started
// pod that belongs to the given job. Returns empty string if no pod is found.
func (c *Client) GetPodLogs(ctx context.Context, jobID string, lines int64) (string, error) {
	pods, err := c.cs.CoreV1().Pods(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("job-id=%s", jobID),
	})
	if err != nil || len(pods.Items) == 0 {
		return "", err
	}

	// Pick the most recently created pod.
	newest := pods.Items[0]
	for _, p := range pods.Items[1:] {
		if p.CreationTimestamp.After(newest.CreationTimestamp.Time) {
			newest = p
		}
	}

	req := c.cs.CoreV1().Pods(c.namespace).GetLogs(newest.Name, &corev1.PodLogOptions{
		TailLines: ptr(lines),
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		return "", fmt.Errorf("stream logs for pod %s: %w", newest.Name, err)
	}
	defer stream.Close()

	buf := make([]byte, 1<<17) // 128 KiB cap
	n, _ := stream.Read(buf)
	return string(buf[:n]), nil
}

// getFailedPodReason looks for pods belonging to the job and returns the first
// non-zero exit or waiting reason it finds.
func (c *Client) getFailedPodReason(ctx context.Context, jobID string) string {
	pods, err := c.cs.CoreV1().Pods(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("job-id=%s", jobID),
	})
	if err != nil || len(pods.Items) == 0 {
		return ""
	}
	for _, pod := range pods.Items {
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.State.Waiting != nil && cs.State.Waiting.Reason != "" {
				msg := cs.State.Waiting.Reason
				if cs.State.Waiting.Message != "" {
					msg += ": " + cs.State.Waiting.Message
				}
				return msg
			}
			if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
				msg := fmt.Sprintf("exited with code %d", cs.State.Terminated.ExitCode)
				if cs.State.Terminated.Message != "" {
					msg += ": " + cs.State.Terminated.Message
				}
				return msg
			}
		}
	}
	return ""
}

// DeleteDeployment removes the worker Deployment.
func (c *Client) DeleteDeployment(ctx context.Context, name string) error {
	err := c.cs.AppsV1().Deployments(c.namespace).Delete(ctx, name, metav1.DeleteOptions{
		PropagationPolicy: ptr(metav1.DeletePropagationForeground),
	})
	if errors.IsNotFound(err) {
		return nil
	}
	return err
}
