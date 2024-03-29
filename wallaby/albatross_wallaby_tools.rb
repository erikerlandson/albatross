# Albatross::WallabyTools - Tools for abstracting wallaby object store operations
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'tmpdir'
require 'tempfile'
require 'logger'
require 'socket'
require 'test/unit'
require 'test/unit/testsuite'


module Albatross
  module ParamUtils
    module ClassMethods
      # accessors for params
      def params=(params)
        @params = params
      end
      def params
        return @params
      end
    end

    # instance accessor for params (params is global to class)
    def params
      return self.class.params
    end

    def try_params(key, dval = nil)
      return dval if not respond_to?(:params)
      return dval if not (params.class <= Hash)
      return dval if not params.has_key?(key)
      return params[key]
    end

    def try_var(v, dval = nil)
      return dval if not instance_variables.include?(v)
      self.instance_eval(v)
    end

    def self.included(base)
      class << base
        include ClassMethods
      end
    end
  end


  module LogUtils
    include ::Albatross::ParamUtils

    def log
      if not instance_variables.include?('@log') then
        @log = Logger.new(try_params(:log_device, STDOUT))
        @log.level = try_params(:log_level, Logger::INFO)
        @log.datetime_format = "%Y-%m-%d %H:%M:%S "
      end
      return @log
    end

    def self.options(opts, pmap)
      opts.separator("\nalbatross logging options")

      pmap[:log_level] = Logger::INFO
      opts.on("--log-level LEVEL", ['debug', 'info', 'warn', 'error', 'fatal', 'unknown'], "{debug|info|warn|error|fatal|unknown}: def= info") do |v|
        v = v.strip.upcase
        if v == "DEBUG" then
          pmap[:log_level] = Logger::DEBUG
        elsif v == "INFO" then
          pmap[:log_level] = Logger::INFO
        elsif v == "WARN" then
          pmap[:log_level] = Logger::WARN
        elsif v == "ERROR" then
          pmap[:log_level] = Logger::ERROR
        elsif v == "FATAL" then
          pmap[:log_level] = Logger::FATAL
        else
          pmap[:log_level] = Logger::UNKNOWN
        end
      end

      pmap[:log_device] = STDOUT
      opts.on("--log-device DEVICE", "set logging device: def= stdout") do |v|
        if v.strip.upcase == "STDOUT" then
          pmap[:log_device] = STDOUT
        elsif v.strip.upcase == "STDERR" then
          pmap[:log_device] = STDERR
        else
          pmap[:log_device] = v
        end
      end
    end
  end

  module Utils
    include ::Albatross::ParamUtils
    include ::Albatross::LogUtils

    def to_array(kwa, p)
      v = kwa[p]
      if v.nil? then
        v = []
      elsif not (v.class <= Array) then
        v = [v]
      end
      kwa[p] = v
    end

    def array_to_s(a)
      "[" + a.join(", ") + "]"
    end

    def fq_hostname
      Socket.gethostbyname(Socket.gethostname).first
    end

    def random_string(len=10)
      (0...len).map{('a'..'z').to_a[rand(26)]}.join
    end
  end # module Utils


  # The purpose of this module is to allow Test::Unit::TestCase objects
  # to have parameters set on them (in this case, via variables on their singleton-class)
  # before an actual instance of the test is declared.  I'm doing this because the Test::Unit
  # framework wants to declare its own instances, so I can't do it and give those instances
  # parameters.  Intead, I give the parameters to the singleton-class (rather like making them
  # global to all instances of the class): so any variables set this way cannot be test-specific.
  module WallabyUnitTestTools
    include ::Albatross::Utils

    module ClassMethods
      # accessors for store
      def store=(store)
        @store = store
      end
      def store
        return @store
      end

      # Override #suite so that it instantiates a single instance
      # of the test class.  Designed to work with the override of .run below
      def suite
        s = ::Test::Unit::TestSuite.new(name)
        # dummy test doesn't actually get run -- it satisfies
        # some sanity checking in Test::Unit that I need to sneak past.
        s << new('__dummy_test__')
        return s
      end
    end


    def self.options(opts, pmap)
      opts.separator("\nalbatross unit testing options")

      pmap[:test] = '.*'
      opts.on("--test REGEXP", "run tests matching REGEXP: def= all") do |v|
        pmap[:test] = v
      end

      pmap[:white] = nil
      opts.on("--white REGEXP", "target node white-list regexp") do |v|
        pmap[:white] = v
      end

      pmap[:black] = nil
      opts.on("--black REGEXP", "target node black-list regexp") do |v|
        pmap[:black] = v
      end

      pmap[:restore] = true
      opts.on("--[no-]restore", "restore pre-test configuration: def= %s" % [pmap[:restore]]) do |v|
        pmap[:restore] = v
      end

      pmap[:pretest] = true
      opts.on("--[no-]pretest", "take pre-test snapshot: def= %s" % [pmap[:pretest]]) do |v|
        pmap[:pretest] = v
      end

      begin
        pmap[:condor_host] = `condor_config_val CONDOR_HOST`.strip
      rescue
        pmap[:condor_host] = fq_hostname
      end
      opts.on("--condor-host HOSTNAME", "condor pool host: def= %s" % [pmap[:condor_host]]) do |v|
        pmap[:condor_host] = v
      end

      ::Albatross::LogUtils.options(opts, pmap)

      opts
    end

    # instance accessor for store (store is global to class)
    def store
      return self.class.store
    end

    def take_snapshot(name, kwa={})
      kwdef = {}
      kwa = kwdef.merge(kwa)
      log.info("snapshotting current store to %s" % [name])
      store.makeSnapshot(name)
    end

    def load_snapshot(name, kwa={})
      kwdef = {}
      kwa = kwdef.merge(kwa)
      log.info("loading snapshot %s" % [name])
      store.loadSnapshot(name)
    end

    # default suite setup/teardown
    def suite_setup
      log.info("%s %s" % [$0, $*.join(" ")])
      @starttime = Time.now.to_i
      @pretest_snapshot_taken = false
      log.debug("WallabyUnitTestTools.suite_setup")
      @tmpdir = Dir.tmpdir + "/awth_" + random_string
      Dir.mkdir(@tmpdir)
      log.info("tmpdir= %s" % [@tmpdir])
      @test_date = Time.now.strftime("%Y/%m/%d_%H:%M:%S")
      @pretest_snapshot = "albatross_wallaby_utt_%s_pretest" % (@test_date)
      ENV['PATH'] = ENV['PATH'] + ":" + ENV['WALLABY_COMMAND_DIR'] + "/../submodules/condor_tools/bin"
      log.debug("Set PATH= %s" % [ENV['PATH']])
      if try_params(:pretest, true) then
        take_snapshot(@pretest_snapshot)
        @pretest_snapshot_taken = true
      end
    end

    def suite_teardown
      log.debug("WallabyUnitTestTools.suite_teardown")
      if try_params(:restore, true) and try_params(:pretest, true) and @pretest_snapshot_taken then
        load_snapshot(@pretest_snapshot)
        store.activateConfiguration()
      end
    end

    # A dummy test to pacify Test::Unit while I subvert it's behavior.
    # This test never actually executes.
    def __dummy_test__
    end

    PASSTHROUGH_EXCEPTIONS = [NoMemoryError, SignalException, Interrupt, SystemExit]

    # Override the standard Test::Unit::TestCase run method, to do two things:
    # a) provide suite_setup/suite_teardown
    # b) allow a class to be instantiated as a single object that runs all its
    # test methods, allowing tests to access shared state (like fixtures).
    def run(result, &progress_block)
      do_tests = true
      begin
        # first do suite setup prior to all tests in this object
        suite_setup
      rescue Exception => e
        log.error("caught exception in suite_setup: %s" % [e.to_s])
        raise if PASSTHROUGH_EXCEPTIONS.include?(e.class)
        do_tests = false
      end

      if do_tests then
        # get the tests defined on this object
        method_names = self.class.public_instance_methods(true)
        tests = method_names.delete_if {|method_name| method_name !~ /^test./}

        re = try_params(:test, ".*")
        tests = tests.delete_if {|name| name !~ Regexp.new(re)}

        # Now run each of those tests, using Test::Unit's test running logic.   Basically
        # this spoof's the TestCase internal convention of a single object per test, by 
        # repeatedly setting @method_name
        tests.sort.each do |t|
          @method_name = t
          super(result, &progress_block)
        end
      end

      begin
        # do suite teardown after all tests in this object have been run
        suite_teardown
      rescue Exception => e
        log.error("caught exception in suite_teardown: %s" % [e.to_s])
        raise if PASSTHROUGH_EXCEPTIONS.include?(e.class)
        return
      end
    end

    def poll_for_process_completion(procs, kwa={})
      kwdef = { :interval => 1, :progress_interval => 30 }
      kwa = kwdef.merge(kwa)

      t0 = Time.now.to_f
      t = 0
      tt = 0

      while true
        sleep(kwa[:interval])
        t += kwa[:interval]
        tt += kwa[:interval]
        if t >= kwa[:progress_interval] then
          t -= kwa[:progress_interval]
          log.info("poll_for_process_completion: %g elapsed" % [tt])
        end

        completed = true
        procs.each do |pid|
          begin
            Process.kill(0, pid)
            completed = false
          rescue
          end
          break if not completed
        end
        break if completed
      end

      Time.now.to_f - t0
    end

    def self.included(base)
      # this opens up singleton-class of who we're being mixed into:
      class << base
        include ClassMethods
        # why doesn't this get picked up when I include ParamUtils above?
        include ::Albatross::ParamUtils::ClassMethods
      end
    end
  end # module WallabyUnitTestTools


  # The WallabyTools module is designed to be mixed-in with a class that provides
  # a wallaby store variable named 'store', for example ::Mrg::Grid::Config::Shell::Command
  # or a class mixed in with WallabyUnitTestTools, above
  module WallabyTools
    include ::Albatross::Utils

    class Exception < ::Exception
    end

    def node_groups(node)
      node = store.getNode(node) if node.class <= String
      (["+++DEFAULT"] + node.memberships + [node.identity_group.name]).map { |gn| store.getGroupByName(gn) }
    end

    def node_features(node)
      f = []
      node_groups(node).each { |g| f |= g.features }
      f.map { |fn| store.getFeature(fn) }
    end


    def clear_nodes(nodes, kwa={})
      kwdef = {}
      kwa = kwdef.merge(kwa)

      nodes = [nodes] unless nodes.class <= Array

      log.info("clear_nodes: clearing %s" % [array_to_s(nodes)])
      nodes.each do |node|
        node = store.getNode(node) if node.class <= String
        log.debug("clear_nodes: clearing node %s configuration" % [node.name])

        node.modifyMemberships('replace', [], {})
        node.identity_group.modifyFeatures('replace', [], {})
        node.identity_group.modifyParams('replace', {}, {})
      end
    end


    def declare_groups(group_names, kwa={})
      kwdef = {}
      kwa = kwdef.merge(kwa)

      group_names = [ group_names ] unless group_names.class <= Array
      group_names = store.checkGroupValidity(group_names)

      log.info("declare_groups: declaring new groups %s" % [array_to_s(group_names)])

      group_names.each do |name|
        store.addExplicitGroup(name)
      end
    end


    def declare_features(feature_names, kwa={})
      kwdef = {}
      kwa = kwdef.merge(kwa)

      feature_names = [ feature_names ] unless feature_names.class <= Array
      feature_names = store.checkFeatureValidity(feature_names)

      log.info("declare_features: declaring new features %s" % [array_to_s(feature_names)])

      feature_names.each do |name|
        store.addFeature(name)
      end
    end


    def set_group_features(group_names, feature_names, kwa={})
      kwdef = { :op => 'replace' }
      kwa = kwdef.merge(kwa)

      group_names = [ group_names ] unless group_names.class <= Array
      feature_names = [ feature_names ] unless feature_names.class <= Array

      missing = store.checkGroupValidity(group_names)
      raise(::Albatross::WallabyTools::Exception, "missing groups: %s" % [array_to_s(missing)]) if not missing.empty?
      missing = store.checkFeatureValidity(feature_names)
      raise(::Albatross::WallabyTools::Exception, "missing features: %s" % [array_to_s(missing)]) if not missing.empty?

      log.info("set_group_features: setting features %s on groups %s" % [array_to_s(feature_names), array_to_s(group_names)])

      group_names.each do |group|
        group = store.getGroupByName(group)
        if kwa[:op] == 'insert' then
          group.modifyFeatures('replace', feature_names + group.features, {})
        else
          group.modifyFeatures(kwa[:op], feature_names, {})
        end
      end
    end


    def set_node_features(node_names, feature_names, kwa={})
      kwdef = { :op => 'replace' }
      kwa = kwdef.merge(kwa)

      node_names = [ node_names ] unless node_names.class <= Array
      feature_names = [ feature_names ] unless feature_names.class <= Array

      missing = store.checkNodeValidity(node_names)
      raise(::Albatross::WallabyTools::Exception, "missing nodes: %s" % [array_to_s(missing)]) if not missing.empty?

      log.info("set_node_features: setting features %s on nodes %s" % [array_to_s(feature_names), array_to_s(node_names)])

      set_group_features(node_names.map { |name| store.getNode(name).identity_group.name }, feature_names, kwa)
    end


    def set_node_groups(node_names, group_names, kwa={})
      kwdef = { :op => 'replace' }
      kwa = kwdef.merge(kwa)

      node_names = [ node_names ] unless node_names.class <= Array
      group_names = [ group_names ] unless group_names.class <= Array

      missing = store.checkNodeValidity(node_names)
      raise(::Albatross::WallabyTools::Exception, "missing nodes: %s" % [array_to_s(missing)]) if not missing.empty?
      missing = store.checkGroupValidity(group_names)
      raise(::Albatross::WallabyTools::Exception, "missing groups: %s" % [array_to_s(missing)]) if not missing.empty?

      log.info("set_node_groups: setting groups %s on nodes %s" % [array_to_s(group_names), array_to_s(node_names)])

      node_names.each do |node|
        node = store.getNode(node)
        if kwa[:op] == 'insert' then
          node.modifyMemberships('replace', group_names + node.memberships, {})
        else
          node.modifyMemberships(kwa[:op], group_names, {})
        end
      end
    end


    def build_feature(feature_name, feature_params, kwa={})
      kwdef = { :op => 'replace' }
      kwa = kwdef.merge(kwa)

      log.info("build_feature: %s" % [feature_name])
      store.addFeature(feature_name) unless store.checkFeatureValidity([feature_name]) == []
      feature = store.getFeature(feature_name)

      store.checkParameterValidity(feature_params.keys).each do|param|
        log.debug("build_feature: declaring parameter %s" % [param])
        store.addParam(param)
      end

      feature.modifyParams(kwa[:op], feature_params)
    end


    def build_access_feature(feature_name, kwa={})
      kwdef = { :condor_host => try_params(:condor_host, Socket.gethostbyname(Socket.gethostname).first),
                :collector_host => nil }
      kwa = kwdef.merge(kwa)
      log.info("build_access_feature: %s condor-host= %s" % [feature_name, kwa[:condor_host]])

      kwa[:collector_host] = kwa[:condor_host] unless kwa[:collector_host]

      params={}
      params["CONDOR_HOST"] = kwa[:condor_host]
      params["COLLECTOR_HOST"] = kwa[:collector_host]
      params["ALLOW_WRITE"] = "*"
      params["ALLOW_READ"] = "*"
      params["SEC_DEFAULT_AUTHENTICATION_METHODS"] = "CLAIMTOBE"
      
      build_feature(feature_name, params)
    end


    def build_execute_feature(feature_name, kwa={})
      kwdef = { :startd => 1, :slots => 1, :dynamic => 0, 
                :dl_append => true, :dedicated => true, :preemption => false, :ad_machine => false }
      kwa = kwdef.merge(kwa)

      log.info("build_execute_feature: %s  startd= %d  slots= %d  dynamic= %d" % [ feature_name, kwa[:startd], kwa[:slots], kwa[:dynamic] ])

      params = {}
      params["USE_PROCD"] = "FALSE"

      if kwa[:dedicated] then
        params["START"] = "TRUE"
        params["SUSPEND"] = "FALSE"
        params["KILL"] = "FALSE"
        params["CONTINUE"] = "TRUE"
        params["WANT_VACATE"] = "FALSE"
        params["WANT_SUSPEND"] = "FALSE"
      end

      if not kwa[:preemption] then
        params["MAXJOBRETIREMENTTIME"] = "3600 * 24"
        params["PREEMPT"] = "FALSE"
        params["PREEMPTION_REQUIREMENTS"] = "FALSE"
        params["RANK"] = "0"
        params["NEGOTIATOR_CONSIDER_PREEMPTION"] = "FALSE"
      end

      if kwa[:dynamic] > 0 then
        params["SLOT_TYPE_1"] = "cpus=%d" % (kwa[:dynamic])
        params["SLOT_TYPE_1_PARTITIONABLE"] = "TRUE"
        params["NUM_SLOTS_TYPE_1"] = "%d" % (kwa[:slots])
        params["NUM_CPUS"] = "%d" % (kwa[:slots] * kwa[:dynamic])
      else
        params["NUM_SLOTS"] = "%d" % (kwa[:slots])
        params["NUM_CPUS"] = "%d" % (kwa[:slots])
      end

      if kwa[:dl_append] then
        daemon_list = ">= "
      else
        daemon_list = "MASTER"
      end

      for s in (0...kwa[:startd])
        tag = "%03d"%(s)
        locname = "STARTD%s"%(tag)
        if (s > 0) or not kwa[:dl_append] then
          daemon_list += ","
        end
        daemon_list += "STARTD%s"%(tag)
        params["STARTD%s"%(tag)] = "$(STARTD)"
        params["STARTD%s_ARGS"%(tag)] = "-f -local-name %s"%(locname)
        params["STARTD.%s.STARTD_NAME"%(locname)] = locname
        params["STARTD.%s.STARTD_ADDRESS_FILE"%(locname)] = "$(LOG)/.startd%s-address"%(tag)
        params["STARTD.%s.STARTD_LOG"%(locname)] = "$(LOG)/StartLog%s"%(tag)
        #params["STARTD.%s.EXECUTE"%(locname)] = "$(EXECUTE)/%s"%(locname)
        if kwa[:ad_machine] then
          params["STARTD%s.STARTD_ATTRS"%(tag)] = "$(STARTD_ATTRS), Machine"
          params["STARTD%s.Machine"%(tag)] = "\"s%s.$(FULL_HOSTNAME)\""%(tag)
        end
      end
      
      params["DAEMON_LIST"] = daemon_list

      build_feature(feature_name, params)

      tslots = kwa[:startd] * kwa[:slots]
      return [ tslots, tslots * kwa[:dynamic] ]
    end


    def build_scheduler_feature(feature_name, kwa={})
      kwdef = { :schedd => 1, :dl_append => true }
      kwa = kwdef.merge(kwa)

      log.info("build_scheduler_feature: %s  schedd= %d" % [ feature_name, kwa[:schedd] ])

      schedd_names = []
      params = {}

      params["USE_PROCD"] = "FALSE"

      if kwa[:dl_append] then
        daemon_list = ">= "
      else
        daemon_list = "MASTER"
      end

      for s in (0...kwa[:schedd])
        tag = "%03d"%(s)
        locname = "SCHEDD%s"%(tag)
        schedd_names += [locname]
        if (s > 0) or not kwa[:dl_append] then
          daemon_list += ","
        end
        daemon_list += "SCHEDD%s"%(tag)
        params["SCHEDD%s"%(tag)] = "$(SCHEDD)"
        params["SCHEDD%s_ARGS"%(tag)] = "-f -local-name %s"%(locname)
        params["SCHEDD.%s.SCHEDD_NAME"%(locname)] = locname
        params["SCHEDD.%s.SCHEDD_LOG"%(locname)] = "$(LOG)/SchedLog%s"%(tag)
        params["SCHEDD.%s.SCHEDD_ADDRESS_FILE"%(locname)] = "$(LOG)/.schedd%s-address"%(tag)
        params["SCHEDD.%s.SPOOL"%(locname)] = "$(SPOOL).%s"%(tag)
        params["SCHEDD.%s.HISTORY"%(locname)] = "$(SPOOL)/history.s%s"%(tag)
      end

      params["DAEMON_LIST"] = daemon_list

      build_feature(feature_name, params)

      return schedd_names
    end


    def build_collector_feature(feature_name, kwa={})
      kwdef = { :collector => 1, :portstart => 10000, :dl_append => true, :disable_plugins => true }
      kwa = kwdef.merge(kwa)

      log.info("build_collector_feature: %s  collector= %d" % [ feature_name, kwa[:collector] ])

      collector_names = []
      params = {}

      if kwa[:dl_append] then
        daemon_list = ">= "
      else
        daemon_list = "MASTER"
      end

      for s in (0...kwa[:collector])
        tag = "%03d"%(s)
        port=kwa[:portstart]+s
        locname = "COLLECTOR%s"%(tag)
        collector_names += [locname]
        if (s > 0) or not kwa[:dl_append] then
          daemon_list += ","
        end
        daemon_list += "COLLECTOR%s"%(tag)
        params["COLLECTOR%s"%(tag)] = "$(COLLECTOR)"
        params["COLLECTOR%s_ARGS"%(tag)] = "-f -p %d -local-name %s" % [ port, locname ]
        params["COLLECTOR%s_ENVIRONMENT"%(tag)] = "_CONDOR_COLLECTOR_LOG=$(LOG)/CollectorLog%s"%(tag)
        params["COLLECTOR.%s.COLLECTOR_NAME"%(locname)] = locname
        params["COLLECTOR.%s.CONDOR_VIEW_HOST"%(locname)] = "$(COLLECTOR_HOST)"
        params["COLLECTOR.%s.COLLECTOR_ADDRESS_FILE"%(locname)] = "$(LOG)/.collector%s-address"%(tag)
        if kwa[:disable_plugins] then
          params["COLLECTOR.%s.PLUGINS"%(locname)] = ""
        end
      end

      params["DAEMON_LIST"] = daemon_list

      build_feature(feature_name, params)

      return collector_names      
    end


    def build_accounting_group_feature(feature_name, group_tuples, kwa={})
      kwdef = { :accept_surplus => false }
      kwa = kwdef.merge(kwa)
      
      log.info("build_accounting_group_feature: %s" % [ feature_name ])

      params = {}
      params["GROUP_NAMES"] = group_tuples.map {|t| t[0]}.join(",")

      params["GROUP_ACCEPT_SURPLUS"] = if kwa[:accept_surplus] then "TRUE" else "FALSE" end

      group_tuples.each do |name, quota, accept_surplus, is_static|
        if is_static == nil then is_static = false end
        if is_static then
          params["GROUP_QUOTA_%s"%(name)] = "%d" % [ quota.to_i ]
        else
          params["GROUP_QUOTA_DYNAMIC_%s"%(name)] = "%f" % [ quota.to_f ]
        end
        
        if accept_surplus == nil then accept_surplus = kwa[:accept_surplus] end
        if accept_surplus != kwa[:accept_surplus] then
          params["GROUP_ACCEPT_SURPLUS_%s"%(name)] = if accept_surplus then "TRUE" else "FALSE" end
        end
      end

      build_feature(feature_name, params)
    end


    def select_nodes(nodes, kwa={})
      kwdef = { :with_feats => nil, :without_feats => nil, :with_groups => nil, :without_groups => nil, 
        :checkin_since => (Time.now.to_f - (3600 + 5*60)), 
        :white => try_params(:white), :black => try_params(:black),
        :allow_condor_host => false, :allow_host => false }
      kwa = kwdef.merge(kwa)

      to_array(kwa, :with_feats)
      to_array(kwa, :without_feats)
      to_array(kwa, :with_groups)
      to_array(kwa, :without_groups)

      log.debug("select_nodes: white= %s" % [kwa[:white]])
      log.debug("select_nodes: black= %s" % [kwa[:black]])

      # subtract the set of nodes that aren't known to the wallaby store:
      s = nodes - store.checkNodeValidity(nodes) 

      s -= [fq_hostname] if not kwa[:allow_host]
      s -= [try_params(:condor_host, fq_hostname)] if not kwa[:allow_condor_host]

      r = []
      s.map {|x| store.getNode(x)}.each do |node|
        checkin = node.last_checkin.to_f / 1000000.0
        log.debug("select_nodes: node name= %s  checkin= %16.6f" % [node.name, checkin])

        # if node hasn't checked in since given time threshold, ignore it
        next if (checkin) < kwa[:checkin_since]

        # black and white lists
        next if kwa[:black] and node.name.match(Regexp.new(kwa[:black]))
        next if kwa[:white] and not node.name.match(Regexp.new(kwa[:white]))

        if (kwa[:with_groups].length > 0) or (kwa[:without_groups].length > 0) then
          g = node_groups(node).map {|x| x.name}
          next if (kwa[:with_groups] - g).length > 0
          next if (kwa[:without_groups] & g).length > 0
        end

        if (kwa[:with_feats].length > 0) or (kwa[:without_feats].length > 0) then
          f = node_features(node).map {|x| x.name}
          next if (kwa[:with_feats] - f).length > 0
          next if (kwa[:without_feats] & f).length > 0
        end

        r << node.name
      end

      return r
    end

    def self.included(base)
      # this opens up singleton-class of who we're being mixed into:
      class << base
        include ::Albatross::ParamUtils::ClassMethods
      end
    end
  end # module WallabyTools


  # Similar to WallabyTools, but for examining condor pools
  module CondorTools
    include ::Albatross::Utils

    class Exception < ::Exception
    end

    # nodes reporting to condor pool
    def condor_nodes(kwa={})
      kwdef = { :with_groups => nil, :constraints => nil }
      kwa = kwdef.merge(kwa)

      to_array(kwa, :with_groups)
      to_array(kwa, :constraints)

      cmd = "condor_status -master"
      cexpr = "True"

      kwa[:with_groups].each do |g|
        cexpr += " && stringListMember(\"%s\", WallabyGroups)" % [g.to_s]
      end

      kwa[:constraints].each do |c|
        cexpr += " && (%s)" % [c.to_s]
      end

      cmd += " -constraint '(%s)'" % [cexpr]

      cmd += " 2>/dev/null"

      log.debug("condor_nodes: cmd= \"%s\"" % [cmd])

      nodes = []
      IO.popen(cmd) do |input|
        nodes = input.read.split("\n")
      end

      return nodes
    end


    def poll_for_slots(nslots, kwa={})
      kwdef = { :group => nil, :interval => 30, :maxtime => 300, :required => nil, :expected => nil }
      kwa = kwdef.merge(kwa)

      cmd = "condor_status -subsystem startd -format \"%s\\n\" Name"
      cmd += " -constraint 'stringListMember(\"%s\", WallabyGroups)'" % [kwa[:group]]  if kwa[:group]
      cmd += " | wc -l"

      log.debug("poll_for_slots: cmd= \"%s\"" % [cmd])

      t0 = Time.now.to_i
      cnodes = nil
      while true
        msg = "Waiting %d seconds for %d slots " % [kwa[:interval], nslots]
        msg += "from group %s " % [kwa[:group]] if kwa[:group]
        msg += "to spool up:"
        log.info(msg)

        sleep(kwa[:interval])

        n = 0
        begin
          IO.popen(cmd) { |input| n = Integer(input.readline.strip) }
        rescue
          n = 0
        end

        elapsed = Time.now.to_i - t0
        log.info("elapsed= %d sec  slots= %d/%d:\n" % [elapsed.to_i, n, nslots])
        break if n >= nslots

        if kwa[:expected] then
          cnodes = condor_nodes(:with_groups => kwa[:group]) unless cnodes
          missing = kwa[:expected] - cnodes
          log.info("missing nodes: %s" % [array_to_s(missing)])
        end
        if elapsed > kwa[:maxtime] then
          break if kwa[:required] and (n >= kwa[:required])
          raise(::Albatross::CondorTools::Exception, "Exceeded maximum polling time %d" % [kwa[:maxtime]])
        end
      end
    end

    def remove_jobs(kwa={})
      kwdef = { :cluster => nil, :tag => nil, :tagvar => "AlbatrossTestTag", :schedd => [] }
      kwa = kwdef.merge(kwa)

      if kwa[:cluster] then
        cmd = "condor_rm -constraint 'ClusterId==%d'" % [kwa[:cluster]]
      elsif kwa[:tag] then
        cmd = "condor_rm -constraint '%s=?=\"%s\"'" % [kwa[:tagvar], kwa[:tag]]
      else
        cmd = "condor_rm -all"
      end

      if kwa[:schedd].length <= 0 then
        log.debug("remove_jobs: cmd= \"%s\"" % [cmd])
        system(cmd)
      else
        kwa[:schedd].each do |name|
          scmd = cmd + (" -name '%s'" % [name])
          log.debug("remove_jobs: cmd= \"%s\"" % [scmd])
          system(scmd)
        end
      end
    end

    def job_count(kwa={})
      kwdef = { :cluster => nil, :tag => nil, :tagvar => "AlbatrossTestTag", :schedd => [], :raise_on_err => false }
      kwa = kwdef.merge(kwa)
      
      if kwa[:cluster] then
        cmd = "condor_q -format \"%%s\\n\" GlobalJobId -constraint 'ClusterId==%d'" % [kwa[:cluster]]
      elsif kwa[:tag] then
        cmd = "condor_q -format \"%%s\\n\" GlobalJobId -constraint '%s=?=\"%s\"'" % [kwa[:tagvar], kwa[:tag]]
      else
        cmd = "condor_q -format \"%s\\n\" GlobalJobId"
      end
      
      if kwa[:schedd].length <= 0 then
        cmd_list = [cmd]
      else
        cmd_list = kwa[:schedd].map { |s| cmd + (" -name '%s'" % [s]) }
      end

      cmd_list = cmd_list.map { |c| c + " | wc -l" }

      n = 0
      cmd_list.each do |cmd|
        t = 0
        begin
          log.debug("job_count: cmd= \"%s\"" % [cmd])
          IO.popen(cmd) { |input| t = Integer(input.readline.strip) }
        rescue
          log.error("job_count: exception on command:\n%s" % [cmd])
          raise if kwa[:raise_on_err]
          t = 0
        end
        log.debug("job_count: schedd jobs= %d" % [t])
        n += t
      end

      n
    end

    def poll_for_empty_job_queue(kwa={})
      kwdef = { :interval => 30, :maxtime => 300, :cluster => nil, :tag => nil, :tagvar => "AlbatrossTestTag", :schedd => [], :remove_jobs => nil}
      kwa = kwdef.merge(kwa)

      begin
        n0 = job_count(kwa.merge({:raise_on_err => true}))
      rescue
        n0 = 99999999
      end

      log.debug("poll_for_empty_job_queue: initial job count= %d" % [n0])

      removed = false
      t0 = Time.now.to_i
      tL = t0
      nL = n0
      while true
        msg = "Waiting %s seconds for job que to clear " % [kwa[:interval]]
        if kwa[:cluster] then
          msg += "for cluster %d" % [kwa[:cluster]]
        elsif kwa[:tag] then
          msg += "for %s==\"%s\"" % [kwa[:tagvar], kwa[:tag]]
        end
        log.info(msg)

        sleep(kwa[:interval])

        begin
          n = job_count(kwa.merge({:raise_on_err => true}))
        rescue
          n = nL
        end

        tC = Time.now.to_i
        elapsed = tC - t0
        elapsedI = tC - tL
        rate = Float(n0-n)/Float(elapsed)
        rateI = Float(nL-n)/Float(elapsedI)
        log.info("elapsed= %d sec   interval= %d sec   jobs= %d   rate= %f  cum-rate= %f:\n" % [Integer(elapsed), Integer(elapsedI), n, rateI, rate])
        break if n <= 0
        raise(::Albatross::CondorTools::Exception, "Exceeded max polling time %d" % [kwa[:maxtime]]) if elapsed > kwa[:maxtime]
        if kwa[:remove_jobs] and (elapsed >= kwa[:remove_jobs]) and not removed then
          log.info("removing jobs from queues at time %d" % [tC])
          remove_jobs(kwa)
          removed = true
        end
        nL = n
        tL = tC
      end
    end

    def collect_history(kwa={})
      kwdef = { :nodes => try_params(:condor_host, fq_hostname), :wdir => Dir.tmpdir, :fname => ("%s/history"%[Dir.tmpdir]) }
      kwa = kwdef.merge(kwa)

      to_array(kwa, :nodes)

      hflist = []
      kwa[:nodes].length.times do |j|
        mach = kwa[:nodes][j]
        hfname = "%s/history%03d" % [kwa[:wdir], j]
        hflist.push(hfname)
        cmd = "/usr/sbin/condor_fetchlog %s HISTORY > %s" % [mach, hfname]
        log.debug("history cmd= %s" % [cmd])
        system(cmd)
      end

      cmd = "cat %s > %s" % [hflist.join(" "), kwa[:fname]]
      log.debug("cat cmd= %s" % [cmd])
      system(cmd)  
    end

    def collect_rates(hfname, kwa={})
      kwdef = { :odir => Dir.tmpdir, :since => 0, :timeslice => 30, :srates => false, :crates => true }
      kwa = kwdef.merge(kwa)

      if kwa[:srates] then
        hof = "%s/hof_%s.dat" % [kwa[:odir], random_string]
        basecmd = "ptplot -noplot -submissions -f %s -since %d -timeslice %d" % [hfname, kwa[:since], kwa[:timeslice]]
        cmd = basecmd + " -hof-out %s" % [hof]
        cmd += " -submissions >%s/submissions.dat" % [kwa[:odir]]
        basecmd += " -hof %s" % [hof]
        log.debug("ptplot cmd= %s" % [cmd])
        system(cmd)
        File.open("%s/submissions.dat" % [kwa[:odir]]) do |input|
          log.info("\nsubmissions:\n%s\n" % [input.read])
        end

        cmd = basecmd + " -submissions -cum >%s/submissions_cum.dat" % [kwa[:odir]]
        log.debug("ptplot cmd= %s" % [cmd])
        system(cmd)
        File.open("%s/submissions_cum.dat" % [kwa[:odir]]) do |input|
          log.info("\nsubmissions cum:\n%s\n" % [input.read])
        end

        cmd = basecmd + " -submissions -rate >%s/submissions_rate.dat" % [kwa[:odir]]
        log.debug("ptplot cmd= %s" % [cmd])
        system(cmd)
        File.open("%s/submissions_rate.dat" % [kwa[:odir]]) do |input|
          log.info("\nsubmissions rate:\n%s\n" % [input.read])
        end

        cmd = basecmd + " -submissions -cum -rate >%s/submissions_cum_rate.dat" % [kwa[:odir]]
        log.debug("ptplot cmd= %s" % [cmd])
        system(cmd)
        File.open("%s/submissions_cum_rate.dat" % [kwa[:odir]]) do |input|
          log.info("\nsubmissions cum rate:\n%s\n" % [input.read])
        end
      end

      if kwa[:crates] then
        hof = "%s/hof_%s.dat" % [kwa[:odir], random_string]
        basecmd = "ptplot -noplot -f %s -since %d -timeslice %d" % [hfname, kwa[:since], kwa[:timeslice]]
        cmd = basecmd + " -hof-out %s" % [hof]
        cmd += " >%s/completions.dat" % [kwa[:odir]]
        basecmd += " -hof %s" % [hof]
        log.debug("ptplot cmd= %s" % [cmd])
        system(cmd)
        File.open("%s/completions.dat" % [kwa[:odir]]) do |input|
          log.info("\ncompletions:\n%s\n" % [input.read])
        end

        cmd = basecmd + " -cum >%s/completions_cum.dat" % [kwa[:odir]]
        log.debug("ptplot cmd= %s" % [cmd])
        system(cmd)
        File.open("%s/completions_cum.dat" % [kwa[:odir]]) do |input|
          log.info("\ncompletions cum:\n%s\n" % [input.read])
        end

        cmd = basecmd + " -rate >%s/completions_rate.dat" % [kwa[:odir]]
        log.debug("ptplot cmd= %s" % [cmd])
        system(cmd)
        File.open("%s/completions_rate.dat" % [kwa[:odir]]) do |input|
          log.info("\ncompletions rate:\n%s\n" % [input.read])
        end

        cmd = basecmd + " -cum -rate >%s/completions_cum_rate.dat" % [kwa[:odir]]
        log.debug("ptplot cmd= %s" % [cmd])
        system(cmd)
        File.open("%s/completions_cum_rate.dat" % [kwa[:odir]]) do |input|
          log.info("\ncompletions cum rate:\n%s\n" % [input.read])
        end
      end
    end

  end # module CondorTools

end # module Albatross
