import sys, os, os.path, string
import time
import datetime
import tempfile
import subprocess
import unittest
import StringIO
import argparse

from wallabyclient.exceptions import *
from wallabyclient import WallabyHelpers, WallabyTypes
from qmf.console import Session


parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('-b', '--broker', dest='broker_addr', default='127.0.0.1', metavar='<host>')
parser.add_argument('-o', '--port', type=int, dest='port', default=5672, metavar='<port>')
parser.add_argument('-P', '--password', dest='passwd', default='', metavar='<password>')
parser.add_argument('-U', '--user', dest='username', default='', metavar='<username>')
parser.add_argument('-m', '--auth-mechanism', dest='mechanisms', default='ANONYMOUS PLAIN GSSAPI', metavar='<mech-name(s)>')
parser.add_argument('-p', '--package', dest='package', default='com.redhat.grid.config', metavar='<package-name>')
parser.add_argument('-c', '--collector', dest='collector_addr', default=None, metavar='<host>')
parser.add_argument('--no-restore', dest='no_restore', action='store_true', default=False, help='do not restore pre-test config')
parser.add_argument('--preload-snapshot', dest='preload_snapshot', default=None, metavar='<snapshot-name>')


supported_api_versions = {20100804:0, 20100915:0, 20101031:1}
connection = None
params = None


def init(p):
    global params
    # At the moment I don't feel sure what the semantics would be for allowing multiple init calls
    if params is not None: raise Exception("params already initialized")
    params = p
    if params.collector_addr is None: params.collector_addr = params.broker_addr


def connect_to_wallaby(broker_addr='127.0.0.1', port=5672, username='', passwd='', mechanisms='ANONYMOUS PLAIN GSSAPI'):
    global supported_api_versions

    # set up session for wallaby
    session = Session()

    if username != '' and passwd != '':
        broker_str = '%s/%s@%s:%d' % (username, passwd, broker_addr, port)
    elif username != '':
        broker_str = '%s@%s:%d' % (username, broker_addr, port)
    else:
        broker_str = '%s:%d' % (broker_addr, port)

    sys.stdout.write("Connecting to broker %s:\n" % (broker_str))

    try:
        broker = session.addBroker('amqp://%s' % broker_str, mechanisms=mechanisms)
    except:
        sys.stderr.write('Unable to connect to broker "%s"\n' % broker_str)
        raise

    # Retrieve the config store object
    sys.stdout.write("Connecting to wallaby store:\n")
    try:
        (store_agent, config_store) = WallabyHelpers.get_store_objs(session)
    except WallabyStoreError, error:
        sys.stderr.write('Error: %s\n' % error.error_str)
        session.delBroker(broker)
        raise

    # Check API version number
    try:
        WallabyHelpers.verify_store_api(config_store, supported_api_versions)
    except WallabyUnsupportedAPI, error:
        if error.minor == 0:
            store_api_version = error.major
        else:
            store_api_version = '%s.%s' % (error.major, error.minor)
        sys.stderr.write('The store is using an API version that is not supported (%s)\n' % store_api_version)
        session.delBroker(broker)
        raise

    # return all the connection objects
    return (session, broker, store_agent, config_store)


# A base class for our unit tests -- defines snapshot/restore for the pool
class condor_unit_test(unittest.TestCase):
    def take_snapshot(self, name):
        sys.stdout.write("Snapshotting current pool config to %s:\n" % (name))
        result = self.config_store.makeSnapshot(name)
        if result.status != 0:
            sys.stderr.write("Failed to snapshot current pool to %s: (%d, %s)\n" % (name, result.status, result.text))
            raise WallabyStoreError(result.text)
        sys.stdout.write("Finished config snapshot %s\n" % (name))

    def load_snapshot(self, name):
        sys.stdout.write("Restoring pool config from %s:\n" % (name))
        result = self.config_store.loadSnapshot(name)
        if result.status != 0:
            sys.stderr.write("Failed to restore from %s: (%d, %s)\n" % (name, result.status, result.text))
            raise WallabyStoreError(result.text)
        sys.stdout.write("Finished restoring snapshot %s\n" % (name))


    def setUp(self):
        self.setup = False

        self.devnull = open(os.devnull, 'rw')
        self.uname = os.uname()
        self.hostname = self.uname[1]

        global params
        if params is None: raise Exception("params uninitialized -- call init() prior to setUp() method")
        self.params = params

        global connection
        if connection == None:
            connection = connect_to_wallaby(broker_addr=params.broker_addr, port=params.port, username=params.username, passwd=params.passwd, mechanisms=params.mechanisms)
        (self.session, self.broker, self.store_agent, self.config_store) = connection

        # take a snapshot before we load any requested pre-config
        self.testdate = time.strftime("%Y/%m/%d_%H:%M:%S")
        self.snapshot = "utcondor_%s_pretest" % (self.testdate)
        self.take_snapshot(self.snapshot)

        # load pre-config snapshot after we snapshot current state
        if self.params.preload_snapshot != None:
            self.load_snapshot(self.params.preload_snapshot)

        try:
            sys.stdout.write("Obtaining nodes from config store:\n")
            node_list = self.store_agent.getObjects(_class='Node', _package=self.params.package)

            sys.stdout.write("Obtaining groups from config store:\n")
            group_list = self.store_agent.getObjects(_class='Group', _package=self.params.package)

            sys.stdout.write("Obtaining features from config store:\n")
            feat_list = self.store_agent.getObjects(_class='Feature', _package=self.params.package)

            sys.stdout.write("Obtaining params from config store:\n")
            param_list = self.store_agent.getObjects(_class='Parameter', _package=self.params.package)
        except:
            sys.stderr.write("Failed to obtain data from current config store\n")
            raise

        self.node_names = [x.name for x in node_list]
        self.group_names = [x.name for x in group_list]
        self.feat_names = [x.name for x in feat_list]
        self.param_names = [x.name for x in param_list]


    def tearDown(self):
        if self.params.no_restore:
            sys.stdout.write("WARNING: NOT restoring pre-test snapshot %s\n" % (self.snapshot))
        else:
            self.load_snapshot(self.snapshot)

            # Activate restored config
            result = self.config_store.activateConfiguration(_timeout=600)
            if result.status != 0:
                sys.stderr.write("Failed to activate restored configuration %s: (%s, %s)\n" % (self.snapshot, result.status, result.text))
                raise Exception(result.text)

        self.session.delBroker(self.broker)


    def assert_param(self, param_name):
        if not param_name in self.param_names:
            sys.stdout.write("Adding parameter %s to store:\n" % (param_name))
            result = self.config_store.addParam(param_name)
            if result.status != 0:
                sys.stderr.write("Failed to add param %s: (%d, %s)\n" % (param_name, result.status, result.text))
                raise WallabyStoreError("Failed to add param")
            self.param_names += [param_name]


    def assert_feature(self, feature_name):
        if not feature_name in self.feat_names:
            result = self.config_store.addFeature(feature_name)
            if result.status != 0:
                sys.stderr.write("Failed to add feature %s: (%s, %s)\n" % (feature_name, result.status, result.text))
                raise WallabyStoreError(result.text)
            self.feat_names += [feature_name]


    def assert_group_features(self, feature_names, group_names, mod_op='replace'):
        # ensure these actually exist
        for grp in group_names:
            if not grp in self.group_names:
                sys.stdout.write("Adding group %s to store:\n" % (grp))
                result = self.config_store.addExplicitGroup(grp)
                if result.status != 0:
                    sys.stderr.write("Failed to create group %s: (%d, %s)\n" % (grp, result.status, result.text))
                    raise WallabyStoreError(result.text)

        # In principle, could automatically install features if they aren't found
        for feat in feature_names:
            if not feat in self.feat_names: raise Exception("Feature %s not in config store" % (feat))

        # apply feature list to group
        for name in group_names:
            group_obj = WallabyHelpers.get_group(self.session, self.config_store, name)
            result = group_obj.modifyFeatures(mod_op, feature_names, {})
            if result.status != 0:
                sys.stderr.write("Failed to set features for %s: (%d, %s)\n" % (name, result.status, result.text))
                raise WallabyStoreError(result.text)


    def assert_node_features(self, feature_names, node_names, mod_op='replace'):
        for feat in feature_names:
            if not feat in self.feat_names:
                emsg = "Feature %s not in config store" % (feat)
                raise Exception(emsg)

        # apply feature list to nodes
        for name in node_names:
            node_obj = WallabyHelpers.get_node(self.session, self.config_store, name)
            group_name = WallabyHelpers.get_id_group_name(node_obj, self.session)
            group_obj = WallabyHelpers.get_group(self.session, self.config_store, group_name)
            if mod_op == 'insert':
                result = group_obj.modifyFeatures('replace', feature_names + group_obj.features, {})
            else:
                result = group_obj.modifyFeatures(mod_op, feature_names, {})
            if result.status != 0:
                sys.stderr.write("Failed to set features for %s: (%d, %s)\n" % (name, result.status, result.text))
                raise WallabyStoreError(result.text)


    def assert_node_groups(self, group_names, node_names, mod_op='replace'):
        # apply the groups to the nodes
        for name in node_names:
            node_obj = WallabyHelpers.get_node(self.session, self.config_store, name)
            result = node_obj.modifyMemberships(mod_op, group_names, {})
            if result.status != 0:
                sys.stderr.write("Failed to set groups for %s: (%d, %s)\n" % (name, result.status, result.text))
                raise WallabyStoreError(result.text)


    def clear_nodes(self, node_names):
        for name in node_names:
            node_obj = WallabyHelpers.get_node(self.session, self.config_store, name)
            result = node_obj.modifyMemberships('replace', [], {})
            if result.status != 0:
                sys.stderr.write("Failed to clear groups from %s: (%d, %s)\n" % (name, result.status, result.text))
                raise WallabyStoreError("Failed to clear groups")

            group_name = WallabyHelpers.get_id_group_name(node_obj, self.session)
            group_obj = WallabyHelpers.get_group(self.session, self.config_store, group_name)
            result = group_obj.modifyFeatures('replace', [], {})
            if result.status != 0:
                sys.stderr.write("Failed to clear features from %s: (%d, %s)\n" % (name, result.status, result.text))
                raise WallabyStoreError("Failed to clear features")

            result = group_obj.modifyParams('replace', {}, {})
            if result.status != 0:
                sys.stderr.write("Failed to clear params from %s: (%d, %s)\n" % (name, result.status, result.text))
                raise WallabyStoreError("Failed to clear params")


    def tag_test_feature(self, feature_name, param_name):
        # ensure parameter name exists
        self.assert_param(param_name)

        # ensure that parameter requires restart
        param_obj = WallabyHelpers.get_param(self.session, self.config_store, param_name)
        result = param_obj.setRequiresRestart(True)
        if result.status != 0:
            sys.stderr.write("Failed to set restart for %s: (%d, %s)\n" % (param_name, result.status, result.text))
            raise WallabyStoreError("Failed to set restart")

        # set this param to a new value, to ensure a restart on activation
        feat_obj = WallabyHelpers.get_feature(self.session, self.config_store, feature_name)
        result = feat_obj.modifyParams('add', {param_name:("%s"%(time.time()))}, {})
        if result.status != 0:
            sys.stderr.write("Failed to add param %s to %s: (%d, %s)\n" % (param_name, feature_name, result.status, result.text))
            raise WallabyStoreError("Failed to add param")

        # make sure master is tagged for restart via this parameter
        subsys_obj = WallabyHelpers.get_subsys(self.session, self.config_store, 'master')
        result = subsys_obj.modifyParams('add', [param_name], {})
        if result.status != 0:
            sys.stderr.write("Failed to add param %s to master: (%d, %s)\n" % (param_name, result.status, result.text))
            raise WallabyStoreError("Failed to add param")


    def poll_for_process_completion(self, procs, interval=1, progress_interval=10):
        t0 = time.time()
        t = 0
        tt = 0
        while True:
            time.sleep(interval)
            t += interval
            tt += interval
            if (t >= progress_interval):
                t -= progress_interval
                sys.stdout.write("..%04d"%(tt))
                sys.stdout.flush()
            completed = True
            for p in procs:
                if p.poll() == None: completed = False
            if completed: break
        sys.stdout.write("\n")
        return time.time() - t0


    def poll_for_slots(self, nslots, group=None, interval=30, maxtime=600, required=None, expected_nodes=None):
        if group == None:
            status_cmd = "condor_status -subsystem startd -format \"%s\\n\" Name | wc -l"
        else:
            status_cmd = "condor_status -subsystem startd -format \"%%s\\n\" Name -constraint 'stringListMember(\"%s\", WallabyGroups)' | wc -l" % (group)
        t0 = time.time()
        while (True):
            sys.stdout.write("Waiting %d seconds for %d slots " % (interval, nslots))
            if group != None: sys.stdout.write("from group %s " % (group))
            sys.stdout.write("to spool up:\n")
            time.sleep(interval)
            try:
                res = subprocess.Popen(["/bin/sh", "-c", status_cmd], stdout=subprocess.PIPE, stderr=self.devnull).communicate()[0]
                res = res.strip()
                n = int(res)
            except:
                n = 0
            elapsed = time.time() - t0
            # stop waiting if we see we have the desired number of configured startds
            sys.stdout.write("elapsed= %d sec  slots= %d:\n" % (int(elapsed), n))
            if n >= nslots: break
            if (elapsed > maxtime):
                if expected_nodes != None:
                    xs = set(expected_nodes)
                    rs = set(self.reporting_nodes(with_groups=group))
                    missing = list(xs - rs)
                    sys.stdout.write("missing nodes: %s\n" % (missing))
                if (required != None) and (n >= required): break
                raise Exception("Exceeded max polling time")


    def remove_jobs(self, cluster=None, tag=None, tagvar="CondorUnitTestTag", schedd=[]):
        if cluster != None:
            rm_cmd = "condor_rm -constraint 'ClusterId==%d'" % (cluster)
        elif tag != None:
            rm_cmd = "condor_rm -constraint '%s=?=\"%s\"'" % (tagvar, tag)
        else:
            rm_cmd = "condor_rm -all"
        if len(schedd) <= 0:
            subprocess.call(["/bin/sh", "-c", rm_cmd], stdout=self.devnull, stderr=self.devnull)
        else:
            for name in schedd:
                rm_cmd_schedd = "%s -name '%s'" % (rm_cmd, name)
                subprocess.call(["/bin/sh", "-c", rm_cmd_schedd], stdout=self.devnull, stderr=self.devnull)
                


    def job_count(self, cluster=None, tag=None, tagvar="CondorUnitTestTag", schedd=[], raise_on_err=False):
        if cluster != None:
            q_cmd = "condor_q -format \"%%s\\n\" GlobalJobId -constraint 'ClusterId==%d'" % (cluster)
        elif tag != None:
            q_cmd = "condor_q -format \"%%s\\n\" GlobalJobId -constraint '%s=?=\"%s\"'" % (tagvar, tag)
        else:
            q_cmd = "condor_q -format \"%s\\n\" GlobalJobId"

        if len(schedd) <= 0:
            q_cmd += " | wc -l"
            q_cmd_list = [q_cmd]
        else:
            q_cmd_list = [q_cmd + " -name '%s' | wc -l" % (s) for s in schedd]

        n = 0
        for q_cmd in q_cmd_list:
            try:
                res = subprocess.Popen(["/bin/sh", "-c", q_cmd], stdout=subprocess.PIPE, stderr=self.devnull).communicate()[0]
                t = int(res)
            except:
                sys.stderr.write("job_count: exception on command:\n%s\n"%(q_cmd))
                if raise_on_err: raise
                t = 0
            n += t

        return n


    def poll_for_empty_job_queue(self, cluster=None, tag=None, tagvar="CondorUnitTestTag", interval=30, maxtime=600, schedd=[]):
        try:
            # get an initial job count.
            n0 = self.job_count(cluster=cluster, tag=tag, tagvar=tagvar, schedd=schedd, raise_on_err=True)
        except:
            n0 = 999999
        t0 = time.time()
        tL = t0
        nL = n0
        while (True):
            sys.stdout.write("Waiting %s seconds for job que to clear " % (interval))
            if cluster != None: sys.stdout.write("for cluster %d " % (cluster))
            elif tag != None: sys.stdout.write("for %s==\"%s\"" % (tagvar, tag))
            sys.stdout.write("\n")
            time.sleep(interval)
            try:
                n = self.job_count(cluster=cluster, tag=tag, tagvar=tagvar, schedd=schedd, raise_on_err=True)
            except:
                n = nL
            tC = time.time()
            elapsed = tC - t0
            elapsedI = tC - tL
            rate = float(n0-n)/float(elapsed)
            rateI = float(nL-n)/float(elapsedI)
            sys.stdout.write("elapsed= %d sec   interval= %d sec   jobs= %d   rate= %f  cum-rate= %f:\n" % (int(elapsed), int(elapsedI), n, rateI, rate))
            # stop waiting when que is clear of specified jobs
            if n <= 0: break
            if (elapsed > maxtime): raise Exception("Exceeded max polling time")
            nL = n
            tL = tC


    def format_opt_list(self, attr_list):
        n = len(attr_list)
        flist = []
        for j in xrange(n):
            fmt = "'"
            if j > 0: fmt += '\t'
            fmt += '%s'
            if j == (n-1): fmt += '\n'
            fmt += "'"
            flist += ['-format %s %s' % (fmt, attr_list[j])]
        return flist


    def reporting_nodes(self, with_groups=None, with_attr=None):
        if with_groups == None: with_groups = []
        elif isinstance(with_groups, str): with_groups = [with_groups]
        elif isinstance(with_groups, set): with_groups = list(with_groups)
        cexpr = " && ".join(["stringListMember(\"%s\", WallabyGroups)" % (g) for g in with_groups])
        
        if with_attr == None: with_attr = []
        elif isinstance(with_attr, str): with_attr = [with_attr]
        elif isinstance(with_attr, set): with_attr = list(with_attr)
        fopt = " ".join(self.format_opt_list(with_attr))

        cmd = "condor_status -master"
        if fopt != "": cmd += " %s" % (fopt)
        if cexpr != "": cmd += " -constraint '%s'" % (cexpr)
        res = subprocess.Popen(["/bin/sh", "-c", cmd], stdout=subprocess.PIPE, stderr=self.devnull).communicate()[0]

        res = res.strip('\n')
        nlist = res.split('\n')
        if len(with_attr) > 0: nlist = [x.split('\t') for x in nlist]
        return nlist


    def list_nodes(self, with_all_feats=None, without_any_feats=None, with_all_groups=None, without_any_groups=None, checkin_since=None):
        r = []
        for node in self.node_names:
            node_obj = WallabyHelpers.get_node(self.session, self.config_store, node)

            sys.stderr.write("    list_nodes: node=%s   checkin= %s\n" % (node, node_obj.last_checkin))

            if (checkin_since != None) and ((node_obj.last_checkin / 1000000) < checkin_since): continue

            nodefeats = []
            if (with_all_feats != None) or (without_any_feats != None):
                nodefeats = WallabyHelpers.get_node_features(node_obj, self.session, self.config_store)
            if (with_all_feats != None) and (False in [x in nodefeats for x in with_all_feats]): continue
            if (without_any_feats != None) and (True in [x in nodefeats for x in without_any_feats]): continue

            nodegroups = []
            if (with_all_groups != None) or (without_any_groups != None):
                nodegroups = [WallabyHelpers.get_id_group_name(node_obj, self.session)] + node_obj.memberships + ['+++DEFAULT']
            if (with_all_groups != None) and (False in [x in nodegroups for x in with_all_groups]): continue
            if (without_any_groups != None) and (True in [x in nodegroups for x in without_any_groups]): continue

            r += [node]
        return r


    def build_feature(self, feature_name, params={}, mod_op='replace'):
        sys.stdout.write("building feature %s\n"%(feature_name))
        self.assert_feature(feature_name)

        # make sure parameters are declared
        splist = params.keys()
        splist.sort()
        for p in splist: self.assert_param(p)

        feat_obj = WallabyHelpers.get_feature(self.session, self.config_store, feature_name)
        result = feat_obj.modifyParams(mod_op, params, {})
        if result.status != 0:
            sys.stderr.write("Failed to modify params for %s: (%d, %s)\n" % (feature_name, result.status, result.text))
            raise WallabyStoreError("Failed to add feature")


    def build_access_feature(self, feature_name, collector_host=None):
        self.assert_feature(feature_name)

        if collector_host==None: collector_host = self.params.collector_addr
        sys.stdout.write("building access feature %s\n"%(feature_name))

        params={}
        params["COLLECTOR_HOST"] = collector_host
        params["ALLOW_WRITE"] = "*"
        params["ALLOW_READ"] = "*"
        params["SEC_DEFAULT_AUTHENTICATION_METHODS"] = "CLAIMTOBE"

        # make sure parameters are declared
        splist = params.keys()
        splist.sort()
        for p in splist: self.assert_param(p)

        feat_obj = WallabyHelpers.get_feature(self.session, self.config_store, feature_name)
        result = feat_obj.modifyParams('replace', params, {})
        if result.status != 0:
            sys.stderr.write("Failed to modify params for %s: (%d, %s)\n" % (feature_name, result.status, result.text))
            raise WallabyStoreError("Failed to add feature")


    def build_execute_feature(self, feature_name, n_startd=1, n_slots=1, n_dynamic=0, dl_append=True):
        self.assert_feature(feature_name)

        sys.stdout.write("building execute feature %s -- n_startd=%d  n_slots=%d  n_dynamic=%d\n"%(feature_name, n_startd, n_slots, n_dynamic))

        params={}
        params["USE_PROCD"] = "FALSE"

        params["START"] = "TRUE"
        params["SUSPEND"] = "FALSE"
        params["KILL"] = "FALSE"
        params["CONTINUE"] = "TRUE"
        params["WANT_VACATE"] = "FALSE"
        params["WANT_SUSPEND"] = "FALSE"

        #params["CLAIM_WORKLIFE"] = "0"
        params["MAXJOBRETIREMENTTIME"] = "3600 * 24"
        params["PREEMPT"] = "FALSE"
        params["PREEMPTION_REQUIREMENTS"] = "FALSE"
        params["RANK"] = "0"
        params["NEGOTIATOR_CONSIDER_PREEMPTION"] = "FALSE"

        if n_dynamic > 0:
            params["SLOT_TYPE_1"] = "cpus=%d"%(n_dynamic)
            params["SLOT_TYPE_1_PARTITIONABLE"] = "TRUE"
            params["NUM_SLOTS_TYPE_1"] = "%d"%(n_slots)
            params["NUM_CPUS"] = "%d"%(n_slots * n_dynamic)
        else:
            params["NUM_SLOTS"] = "%d"%(n_slots)
            params["NUM_CPUS"] = "%d"%(n_slots)

        if dl_append: daemon_list = ">= "
        else:         daemon_list = "MASTER"
        for s in xrange(n_startd):
            tag = "ST%03d"%(s)
            if (s > 0) or not dl_append: daemon_list += ","
            daemon_list += "STARTD_%s"%(tag)
            params["STARTD_%s"%(tag)] = "$(STARTD)"
            params["STARTD_%s_ARGS"%(tag)] = "-f -local-name %s"%(tag)
            params["STARTD.%s.STARTD_NAME"%(tag)] = "%s"%(tag)
            params["STARTD.%s.ADDRESS_FILE"%(tag)] = "$(LOG)/.%s-address"%(tag)
            params["STARTD.%s.STARTD_LOG"%(tag)] = "$(LOG)/%s_Log"%(tag)

        params["DAEMON_LIST"] = daemon_list

        # make sure parameters are declared
        splist = params.keys()
        splist.sort()
        for p in splist: self.assert_param(p)

        feat_obj = WallabyHelpers.get_feature(self.session, self.config_store, feature_name)
        result = feat_obj.modifyParams('replace', params, {})
        if result.status != 0:
            sys.stderr.write("Failed to modify params for %s: (%d, %s)\n" % (feature_name, result.status, result.text))
            raise WallabyStoreError("Failed to add feature")

        tslots = n_startd * n_slots
        return (tslots, tslots * n_dynamic)


    def build_scheduler_feature(self, feature_name, n_schedd=1, dl_append=True):
        sys.stdout.write("building scheduler feature %s with %d schedds\n"%(feature_name, n_schedd))
        self.assert_feature(feature_name)

        params={}
        params["USE_PROCD"] = "FALSE"
        if dl_append: daemon_list = ">= "
        else:         daemon_list = "MASTER"
        for s in xrange(n_schedd):
            tag = "SCH%03d"%(s)
            if (s > 0) or not dl_append: daemon_list += ","
            daemon_list += "SCHEDD_%s"%(tag)
            params["SCHEDD_%s"%(tag)] = "$(SCHEDD)"
            params["SCHEDD_%s_ARGS"%(tag)] = "-f -local-name %s"%(tag)
            params["SCHEDD.%s.SCHEDD_NAME"%(tag)] = "%s"%(tag)
            params["SCHEDD.%s.ADDRESS_FILE"%(tag)] = "$(LOG)/.%s-address"%(tag)
            params["SCHEDD.%s.SCHEDD_LOG"%(tag)] = "$(LOG)/%s_Log"%(tag)

        params["DAEMON_LIST"] = daemon_list

        # make sure parameters are declared
        splist = params.keys()
        splist.sort()
        for p in splist: self.assert_param(p)

        feat_obj = WallabyHelpers.get_feature(self.session, self.config_store, feature_name)
        result = feat_obj.modifyParams('replace', params, {})
        if result.status != 0:
            sys.stderr.write("Failed to modify params for %s: (%d, %s)\n" % (feature_name, result.status, result.text))
            raise WallabyStoreError("Failed to add feature")
