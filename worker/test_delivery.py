import base64
import json
import unittest
from unittest.mock import Mock
from worker.delivery import Processor, RetryPolicy, DeliveryBlocked, LeaseLost, Identity, dead_letter_envelope, error_kind


class Message:
    def __init__(self,value=b'{"id":1}',key=b'key',headers=None):self.body,self.raw_key,self.raw_headers=value,key,headers or []
    def value(self):return self.body
    def key(self):return self.raw_key
    def headers(self):return self.raw_headers
    def topic(self):return 'orders'
    def partition(self):return 0
    def offset(self):return 7
    def timestamp(self):return (1,1700000000000)


class DeliveryPolicyTests(unittest.TestCase):
    def processor(self,destination,commit=None,**kwargs):
        return Processor(destination,commit or Mock(),retries=RetryPolicy(3,0,0),sleep=lambda _:None,**kwargs)

    def test_transient_failures_exhaust_bound_without_commit(self):
        destination=Mock();destination.write.side_effect=ConnectionError('do-not-log-password')
        commit=Mock();events=[]
        with self.assertRaises(DeliveryBlocked) as result:self.processor(destination,commit,emit=events.append).process(Message(),'cluster')
        self.assertEqual(result.exception.stage,'destination');self.assertEqual(destination.write.call_count,3)
        commit.assert_not_called();self.assertNotIn('do-not-log-password',json.dumps(events))

    def test_permanent_invalid_record_pauses_immediately(self):
        destination=Mock();commit=Mock()
        with self.assertRaises(DeliveryBlocked) as result:self.processor(destination,commit).process(Message(b'{broken'),'cluster')
        self.assertEqual(result.exception.stage,'validation');destination.write.assert_not_called();commit.assert_not_called()

    def test_only_acknowledged_dlq_can_precede_commit(self):
        order=[];destination=Mock()
        dlq=lambda envelope:order.append(('dlq_ack',envelope['source']['offset']))
        commit=lambda identity:order.append(('commit',identity.offset+1))
        outcome=self.processor(destination,commit,policy='dead_letter',dead_letter=dlq).process(Message(b'{broken'),'cluster')
        self.assertEqual(order,[('dlq_ack',7),('commit',8)]);self.assertEqual(outcome,'dead_lettered')
        destination.write.assert_not_called()

    def test_dlq_failure_never_advances_source(self):
        dlq=Mock(side_effect=ConnectionError('offline'));commit=Mock()
        with self.assertRaises(DeliveryBlocked) as result:self.processor(Mock(),commit,policy='dead_letter',dead_letter=dlq).process(Message(b'{broken'),'cluster')
        self.assertEqual(result.exception.stage,'dead_letter');self.assertEqual(dlq.call_count,3);commit.assert_not_called()

    def test_commit_failure_is_not_a_record_to_dead_letter(self):
        destination=Mock();destination.write.return_value='applied';dlq=Mock();commit=Mock(side_effect=RuntimeError('coordinator unavailable'))
        with self.assertRaises(DeliveryBlocked) as result:self.processor(destination,commit,policy='dead_letter',dead_letter=dlq).process(Message(),'cluster')
        self.assertEqual(result.exception.stage,'offset_commit');self.assertEqual(destination.write.call_count,1)
        self.assertEqual(commit.call_count,3);dlq.assert_not_called()

    def test_assignment_change_after_write_never_commits_stale_lease(self):
        owned=[True];destination=Mock();destination.write.return_value='applied';commit=Mock()
        processor=self.processor(destination,commit,after_write=lambda identity:owned.__setitem__(0,False))
        with self.assertRaises(LeaseLost):processor.process(Message(),'cluster',owns=lambda:owned[0])
        commit.assert_not_called()

    def test_infrastructure_misconfiguration_is_not_sent_to_dlq(self):
        class AuthenticationFailure(Exception):pgcode='28P01'
        destination=Mock();destination.write.side_effect=AuthenticationFailure('private-driver-message');dlq=Mock();commit=Mock()
        with self.assertRaises(DeliveryBlocked):self.processor(destination,commit,policy='dead_letter',dead_letter=dlq).process(Message(),'cluster')
        self.assertEqual(destination.write.call_count,1);dlq.assert_not_called();commit.assert_not_called()

    def test_dlq_preserves_tombstone_and_duplicate_binary_headers(self):
        message=Message(None,None,[('same',b'\xff'),('same',b'other'),('null',None)])
        envelope=dead_letter_envelope(message,Identity('cluster','orders',0,7),ValueError(),'validation')
        self.assertIsNone(envelope['record']['value_base64']);self.assertIsNone(envelope['record']['key_base64'])
        self.assertEqual(len(envelope['record']['headers']),3)
        self.assertEqual(base64.b64decode(envelope['record']['headers'][0]['value_base64']),b'\xff')
        self.assertNotEqual(Identity('one','orders',0,7).key,Identity('two','orders',0,7).key)
        self.assertNotEqual(Identity('one','orders',0,7).key,Identity('one','payments',0,7).key)

    def test_retry_configuration_cannot_be_unbounded(self):
        for args in [(0,0,0),(6,0,0),(3,-1,0),(3,1,100)]:
            with self.assertRaises(ValueError):RetryPolicy(*args)

if __name__=='__main__':unittest.main()
