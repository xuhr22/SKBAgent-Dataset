
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import avro_utils
from . import AvroTester
from avro.ipc import AvroRemoteException

class TestSystemOperations(AvroTester):
    """
    Cassandra system operations.
    """
    def test_system_keyspace_operations(self):
        "adding, renaming, and removing keyspaces"
        
        # create
        keyspace = dict()
        keyspace['name'] = 'CreateKeyspace'
        keyspace['strategy_class'] = 'org.apache.cassandra.locator.SimpleStrategy'
        keyspace['replication_factor'] = 1
        keyspace['strategy_options'] = {}
        cfdef = dict();
        cfdef['keyspace'] = 'CreateKeyspace'
        cfdef['name'] = 'CreateKsCf'
        keyspace['cf_defs'] = [cfdef]

        #test invalid strategy class
        keyspace['strategy_class'] = 'InvalidStrategy'
        avro_utils.assert_raises(AvroRemoteException,
                self.client.request,
                'system_add_keyspace',
                {'ks_def': keyspace})

        keyspace['strategy_class'] = 'org.apache.cassandra.locator.SimpleStrategy'
        s = self.client.request('system_add_keyspace', {'ks_def' : keyspace})
        assert isinstance(s, unicode), 'returned type is %s, (not \'unicode\')' % type(s)
        
        self.client.request('set_keyspace', {'keyspace' : 'CreateKeyspace'})
        
        # modify valid
        modified_keyspace = {'name': 'CreateKeyspace', 
                             'strategy_class': 'org.apache.cassandra.locator.OldNetworkTopologyStrategy',
                             'strategy_options': {}, 
                             'replication_factor': 1, 
                             'cf_defs': []}
        self.client.request('system_update_keyspace', {'ks_def': modified_keyspace})
        modks = self.client.request('describe_keyspace', {'keyspace': 'CreateKeyspace'})
        assert modks['replication_factor'] == modified_keyspace['replication_factor']
        assert modks['strategy_class'] == modified_keyspace['strategy_class']
        
        # drop
        s = self.client.request('system_drop_keyspace', {'keyspace' : 'CreateKeyspace'})
        assert isinstance(s, unicode), 'returned type is %s, (not \'unicode\')' % type(s)
        avro_utils.assert_raises(AvroRemoteException,
                      self.client.request,
                      'describe_keyspace',
                      {'keyspace' : 'CreateKeyspace'})
        
    def test_system_column_family_operations(self):
        "adding, renaming, and removing column families"
        self.client.request('set_keyspace', {'keyspace': 'Keyspace1'})
        
        # create
        columnDef = dict()
        columnDef['name'] = b'ValidationColumn'
        columnDef['validation_class'] = 'BytesType'
        
        cfDef = dict()
        cfDef['keyspace'] = 'Keyspace1'
        cfDef['name'] = 'NewColumnFamily'
        cfDef['column_metadata'] = [columnDef]
        
        s = self.client.request('system_add_column_family', {'cf_def' : cfDef})
        assert isinstance(s, unicode), \
            'returned type is %s, (not \'unicode\')' % type(s)
        
        ks1 = self.client.request(
            'describe_keyspace', {'keyspace' : 'Keyspace1'})
        assert 'NewColumnFamily' in [x['name'] for x in ks1['cf_defs']]
        cfDef = [x for x in ks1['cf_defs'] if x['name']=='NewColumnFamily'][0]
        assert cfDef['id'] > 1000, str(cfid)

        # modify invalid
        cfDef['comparator_type'] = 'LongType' 
        avro_utils.assert_raises(AvroRemoteException,
                self.client.request,
                'system_update_column_family',
                {'cf_def': cfDef})
        
        # modify valid
        cfDef['comparator_type'] = 'BytesType' # revert back to old value.
        cfDef['row_cache_size'] = 25
        cfDef['gc_grace_seconds'] = 1
        self.client.request('system_update_column_family', {'cf_def': cfDef})
        ks1 = self.client.request('describe_keyspace', {'keyspace': 'Keyspace1'})
        server_cf = [x for x in ks1['cf_defs'] if x['name']=='NewColumnFamily'][0]
        assert server_cf
        assert server_cf['row_cache_size'] == 25
        assert server_cf['gc_grace_seconds'] == 1

        # drop
        self.client.request('system_drop_column_family',
            {'column_family' : 'NewColumnFamily'})
        ks1 = self.client.request(
                'describe_keyspace', {'keyspace' : 'Keyspace1'})
        assert 'NewColumnFamily' not in [x['name'] for x in ks1['cf_defs']]
        assert 'Standard1' in [x['name'] for x in ks1['cf_defs']]
    
