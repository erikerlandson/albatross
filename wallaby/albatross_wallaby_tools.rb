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

require 'socket'
require 'test/unit'
require 'test/unit/testsuite'


module Albatross

  def self.to_array(kwa, p)
    v = kwa[p]
    if v.nil? then
      v = []
    elsif not (v.class <= Array) then
      v = [v]
    end
    kwa[p] = v
  end


  module ParamTools
    def try_params(key, dval = nil)
      return dval if not respond_to?(:params)
      return dval if not (params.class <= Hash)
      return dval if not params.has_key?(key)
      return params[key]
    end
  end


  # The purpose of this module is to allow Test::Unit::TestCase objects
  # to have parameters set on them (in this case, via variables on their singleton-class)
  # before an actual instance of the test is declared.  I'm doing this because the Test::Unit
  # framework wants to declare its own instances, so I can't do it and give those instances
  # parameters.  Intead, I give the parameters to the singleton-class (rather like making them
  # global to all instances of the class): so any variables set this way cannot be test-specific.
  module WallabyUnitTestTools
    include ParamTools

    module ClassMethods
      # accessors for store
      def store=(store)
        @store = store
      end
      def store
        return @store
      end

      # accessors for params
      def params=(params)
        @params = params
      end
      def params
        return @params
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
      pmap[:verbosity] = 0
      opts.on("-v", "--verbose", "verbose output") do
        pmap[:verbosity] = 1
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

      pmap[:condor_host] = Socket.gethostbyname(Socket.gethostname).first
      opts.on("--condor-host HOSTNAME", "condor pool host: def= %s" % [pmap[:condor_host]]) do |v|
        pmap[:condor_host] = v
      end

      opts
    end

    # instance accessor for store (store is global to class)
    def store
      return self.class.store
    end
    # instance accessor for params (params is global to class)
    def params
      return self.class.params
    end

    def take_snapshot(name, kwa={})
      kwdef = { :verbosity => try_params(:verbosity, 0) }
      kwa = kwdef.merge(kwa)
      puts "snapshotting current store to %s" % [name] if kwa[:verbosity] > 0
      store.makeSnapshot(name)
    end

    def load_snapshot(name, kwa={})
      kwdef = { :verbosity => try_params(:verbosity, 0) }
      kwa = kwdef.merge(kwa)
      puts "loading snapshot %s" % [name] if kwa[:verbosity] > 0
      store.loadSnapshot(name)
    end

    # default suite setup/teardown
    def suite_setup
      puts "WallabyUnitTestTools.suite_setup" if try_params(:verbosity, 0) > 0
      @fq_hostname = Socket.gethostbyname(Socket.gethostname).first
      @test_date = Time.now.strftime("%Y/%m/%d_%H:%M:%S")
      @pretest_snapshot = "albatross_wallaby_utt_%s_pretest" % (@test_date)
      take_snapshot(@pretest_snapshot)
    end

    def suite_teardown
      puts "WallabyUnitTestTools.suite_teardown" if try_params(:verbosity, 0) > 0
      if try_params(:restore, true) then
        load_snapshot(@pretest_snapshot)
        config.activateConfiguration(_timeout=60)
      end
    end

    # A dummy test to pacify Test::Unit while I subvert it's behavior.
    # This test never actually executes.
    def __dummy_test__
    end

    # Override the standard Test::Unit::TestCase run method, to do two things:
    # a) provide suite_setup/suite_teardown
    # b) allow a class to be instantiated as a single object that runs all its
    # test methods, allowing tests to access shared state (like fixtures).
    def run(result, &progress_block)
      # first do suite setup prior to all tests in this object
      suite_setup

      # get the tests defined on this object
      method_names = self.class.public_instance_methods(true)
      tests = method_names.delete_if {|method_name| method_name !~ /^test./}

      # Now run each of those tests, using Test::Unit's test running logic.   Basically
      # this spoof's the TestCase internal convention of a single object per test, by 
      # repeatedly setting @method_name
      tests.sort.each do |t|
        @method_name = t
        super(result, &progress_block)
      end

      # do suite teardown after all tests in this object have been run
      suite_teardown
    end

    def self.included(base)
      # this opens up singleton-class of who we're being mixed into:
      class << base
        # this mixes the stuff in ClassMethods into the singleton-class
        # of any class that WallabyUnitTestTools gets mixed into:
        include ClassMethods
      end
    end
  end


  # The WallabyTools module is designed to be mixed-in with a class that provides
  # a wallaby store variable named 'store', for example ::Mrg::Grid::Config::Shell::Command
  # or a class mixed in with WallabyUnitTestTools, above
  module WallabyTools
    include ::Albatross::ParamTools

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
      kwdef = { :verbosity => try_params(:verbosity, 0) }
      kwa = kwdef.merge(kwa)

      nodes.each do |node|
        node = store.getNode(node) if node.class <= String
        puts "clear_nodes: clearing node %s configuration" % [node.name] if kwa[:verbosity] > 0

        node.modifyMemberships('replace', [])
        node.identity_group.modifyFeatures('replace', [])
        node.identity_group.modifyParams('replace', {})
      end
    end


    def set_group_features(feature_names, group_names, kwa={})
      kwdef = { :verbosity => try_params(:verbosity, 0), :op => 'replace' }
      kwa = kwdef.merge(kwa)

      feature_names = [ feature_names ] unless feature_names.class <= Array
      group_names = [ group_names ] unless group_names.class <= Array

      missing = store.checkGroupValidity(group_names)
      raise(::Albatross::Wallaby::Exception, "missing groups: %s" % [missing.join(" ")]) if not missing.empty?
      missing = store.checkFeatureValidity(feature_names)
      raise(::Albatross::Wallaby::Exception, "missing features: %s" % [missing.join(" ")]) if not missing.empty?

      group_names.each do |group|
        group = store.getGroupByName(group)
        group.modifyFeatures(kwa[:op], feature_names)
      end
    end


    def build_feature(feature_name, feature_params, kwa={})
      kwdef = { :op => 'replace', :verbosity => try_params(:verbosity, 0) }
      kwa = kwdef.merge(kwa)

      puts "build_feature: %s" % feature_name if kwa[:verbosity] > 0
      store.addFeature(feature_name) unless store.checkFeatureValidity([feature_name]) == []
      feature = store.getFeature(feature_name)

      store.checkParameterValidity(feature_params.keys).each do|param|
        puts "build_feature: declaring parameter %s" % param if kwa[:verbosity] > 0
        store.addParam(param)
      end

      feature.modifyParams(kwa[:op], feature_params)
    end


    def build_access_feature(feature_name, kwa={})
      kwdef = { :verbosity => try_params(:verbosity, 0), 
                :condor_host => try_params(:condor_host, Socket.gethostbyname(Socket.gethostname).first),
                :collector_host => nil }
      kwa = kwdef.merge(kwa)
      puts "build_access_feature: %s condor-host= %s" % [feature_name, kwa[:condor_host]] if kwa[:verbosity] > 0

      kwa[:collector_host] = kwa[:condor_host] unless kwa[:collector_host]

      params={}
      params["CONDOR_HOST"] = kwa[:condor_host]
      params["COLLECTOR_HOST"] = kwa[:collector_host]
      params["ALLOW_WRITE"] = "*"
      params["ALLOW_READ"] = "*"
      params["SEC_DEFAULT_AUTHENTICATION_METHODS"] = "CLAIMTOBE"
      
      build_feature(feature_name, params, :verbosity => kwa[:verbosity])
    end


    def build_execute_feature(feature_name, kwa={})
      kwdef = { :verbosity => try_params(:verbosity, 0), :startd => 1, :slots => 1, :dynamic => 0, 
                :dl_append => true, :dedicated => true, :preemption => false, :ad_machine => false }
      kwa = kwdef.merge(kwa)

      if kwa[:verbosity] > 0 then 
        puts "build_execute_feature: %s  startd= %d  slots= %d  dynamic= %d" % [ feature_name, kwa[:startd], kwa[:slots], kwa[:dynamic] ]
      end

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

      build_feature(feature_name, params, :verbosity => kwa[:verbosity])

      tslots = kwa[:startd] * kwa[:slots]
      return [ tslots, tslots * kwa[:dynamic] ]
    end


    def build_scheduler_feature(feature_name, kwa={})
      kwdef = { :verbosity => try_params(:verbosity, 0), :schedd => 1, :dl_append => true }
      kwa = kwdef.merge(kwa)

      if kwa[:verbosity] > 0 then
        puts "build_scheduler_feature: %s  schedd= %d" % [ feature_name, kwa[:schedd] ]
      end

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

      build_feature(feature_name, params, :verbosity => kwa[:verbosity])

      return schedd_names
    end


    def build_collector_feature(feature_name, kwa={})
      kwdef = { :verbosity => try_params(:verbosity, 0), :collector => 1, :portstart => 10000, :dl_append => true, :disable_plugins => true }
      kwa = kwdef.merge(kwa)

      if kwa[:verbosity] > 0 then
        puts "build_collector_feature: %s  collector= %d" % [ feature_name, kwa[:collector] ]
      end

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

      build_feature(feature_name, params, :verbosity => kwa[:verbosity])

      return collector_names      
    end


    def build_accounting_group_feature(feature_name, group_tuples, kwa={})
      kwdef = { :verbosity => try_params(:verbosity, 0), :accept_surplus => false }
      kwa = kwdef.merge(kwa)
      
      if kwa[:verbosity] > 0 then
        puts "build_accounting_group_feature: %s" % [ feature_name ]
      end

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

      build_feature(feature_name, params, :verbosity => kwa[:verbosity])
    end


    def select_nodes(nodes, kwa={})
      kwdef = { :verbosity => try_params(:verbosity, 0), :with_feats => nil, :without_feats => nil, :with_groups => nil, :without_groups => nil, 
        :checkin_since => (Time.now.to_f - (3600 + 5*60)), 
        :white => try_params(:white), :black => try_params(:black) }
      kwa = kwdef.merge(kwa)

      Albatross.to_array(kwa, :with_feats)
      Albatross.to_array(kwa, :without_feats)
      Albatross.to_array(kwa, :with_groups)
      Albatross.to_array(kwa, :without_groups)

      # subtract the set of nodes that aren't known to the wallaby store:
      s = nodes - store.checkNodeValidity(nodes) 

      r = []
      s.map {|x| store.getNode(x)}.each do |node|
        checkin = node.last_checkin.to_f / 1000000.0
        puts "node name= %s  checkin= %16.6f" % [node.name, checkin] if kwa[:verbosity] > 0

        # if node hasn't checked in since given time threshold, ignore it
        next if (checkin) < kwa[:checkin_since]

        # black and white lists
        next if kwa[:black] and node.name.match(Regexp.new("^"+kwa[:black]+"$"))
        next if kwa[:white] and not node.name.match(Regexp.new("^"+kwa[:white]+"$"))

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

  end # module WallabyTools


  # Similar to WallabyTools, but for examining condor pools
  module CondorTools
    include ParamTools

    class Exception < ::Exception
    end

    # nodes reporting to condor pool
    def condor_nodes(kwa={})
      kwdef = { :verbosity => try_params(:verbosity, 0), :with_groups => nil, :constraints => nil }
      kwa = kwdef.merge(kwa)

      Albatross.to_array(kwa, :with_groups)
      Albatross.to_array(kwa, :constraints)

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

      if kwa[:verbosity] > 0 then
        puts "condor_nodes: cmd= %s" % [cmd]
      end

      nodes = []
      IO.popen(cmd) do |input|
        nodes = input.read.split("\n")
      end

      return nodes
    end
  end

end # module Albatross
