package kafka

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/IBM/sarama"
	"testing"
)

type groupAdmin struct {
	sarama.ClusterAdmin
	descriptions []*sarama.GroupDescription
	offsets      map[string]*sarama.OffsetFetchResponse
}

func (a groupAdmin) ListConsumerGroups() (map[string]string, error) {
	result := map[string]string{}
	for _, g := range a.descriptions {
		result[g.GroupId] = "consumer"
	}
	return result, nil
}
func (a groupAdmin) DescribeConsumerGroups([]string) ([]*sarama.GroupDescription, error) {
	return a.descriptions, nil
}
func (a groupAdmin) ListConsumerGroupOffsets(group string, _ map[string][]int32) (*sarama.OffsetFetchResponse, error) {
	return a.offsets[group], nil
}

type groupClient struct {
	sarama.Client
	err error
}

func (c groupClient) Partitions(string) ([]int32, error)            { return []int32{0, 1}, c.err }
func (c groupClient) GetOffset(string, int32, int64) (int64, error) { return 100, c.err }
func member(topic string) *sarama.GroupMemberDescription {
	var raw bytes.Buffer
	binary.Write(&raw, binary.BigEndian, int16(0))
	binary.Write(&raw, binary.BigEndian, int32(1))
	binary.Write(&raw, binary.BigEndian, int16(len(topic)))
	raw.WriteString(topic)
	binary.Write(&raw, binary.BigEndian, int32(-1))
	return &sarama.GroupMemberDescription{MemberId: "member", MemberMetadata: raw.Bytes()}
}
func response(topic string, offset int64) *sarama.OffsetFetchResponse {
	return &sarama.OffsetFetchResponse{Blocks: map[string]map[int32]*sarama.OffsetFetchResponseBlock{topic: {0: {Offset: offset}, 1: {Offset: -1}}}}
}
func TestTopicConsumersMatchRetainedOffsetsAndActiveSubscriptions(t *testing.T) {
	admin := groupAdmin{descriptions: []*sarama.GroupDescription{
		{GroupId: "retained", State: "Empty"},
		{GroupId: "active-no-commit", State: "Stable", Members: map[string]*sarama.GroupMemberDescription{"member": member("orders")}},
		{GroupId: "unrelated", State: "Empty"},
	}, offsets: map[string]*sarama.OffsetFetchResponse{"retained": response("orders", 80), "active-no-commit": {Blocks: map[string]map[int32]*sarama.OffsetFetchResponseBlock{}}, "unrelated": response("payments", 20)}}
	client := &AdminClient{admin: admin, client: groupClient{}}
	groups, err := client.ListTopicConsumers("orders")
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 2 {
		t.Fatalf("wrong topic association: %+v", groups)
	}
	if groups[0].GroupID != "active-no-commit" || groups[0].UncommittedPartitions != 2 {
		t.Fatal("active uncommitted subscription lost")
	}
	if groups[1].TopicLag != 20 || groups[1].UncommittedPartitions != 1 {
		t.Fatal("incorrect known lag or unknown commits")
	}
	admin.descriptions = []*sarama.GroupDescription{{GroupId: "retained", State: "Empty"}}
	client.admin = admin
	detail, err := client.GetConsumerGroupDetail("retained")
	if err != nil {
		t.Fatal(err)
	}
	if detail.TopicLag[0].TotalLag != 20 || detail.TopicLag[0].Partitions[1].Lag != -1 {
		t.Fatal("unknown lag presented as zero or added to total")
	}
	client.client = groupClient{err: errors.New("broker unavailable")}
	if _, err := client.ListTopicConsumers("orders"); err == nil {
		t.Fatal("broker failure hidden as empty consumer list")
	}
}
func TestConsumerPermissionsFailClosed(t *testing.T) {
	client := &AdminClient{client: groupClient{}, admin: groupAdmin{descriptions: []*sarama.GroupDescription{{GroupId: "denied"}}, offsets: map[string]*sarama.OffsetFetchResponse{"denied": {Err: sarama.ErrGroupAuthorizationFailed}}}}
	if _, err := client.ListTopicConsumers("orders"); err == nil {
		t.Fatal("permission failure presented as zero lag")
	}
}
