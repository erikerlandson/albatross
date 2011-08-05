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

module Albatross

  # The purpose of this module is to allow Test::Unit::TestCase objects
  # to have parameters set on them (in this case, via variables on their singleton-class)
  # before an actual instance of the test is declared.  I'm doing this because the Test::Unit
  # framework wants to declare its own instances, so I can't do it and give those instances
  # parameters.  Intead, I give the parameters to the singleton-class (rather like making them
  # global to all instances of the class): so any variables set this way cannot be test-specific.
  module WallabyUnitTestTools
    module ClassMethods
      def store=(store)
        @store = store
      end

      def store
        return @store
      end
    end

    # this mixes the stuff in ClassMethods into the singleton object
    # of any class that WallabyUnitTestTools gets mixed into.
    def self.included(base)
      class << base ; include ClassMethods ; end
    end

    # returns the value from the singleton object (global to class)
    def store
      return self.class.store
    end
  end

  # The WallabyTools module is designed to be mixed-in with a class that provides
  # a wallaby store variable named 'store', for example ::Mrg::Grid::Config::Shell::Command
  # or a class mixed in with WallabyUnitTestTools, above
  module WallabyTools

    def build_feature(feature_name, feature_params, kwa={})
      kwdef = { :op => 'replace', :verbosity => 0 }
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


    def build_execute_feature(feature_name, kwa={})
      kwdef = { :verbosity => 0, :startd => 1, :slots => 1, :dynamic => 0, 
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
      kwdef = { :verbosity => 0, :schedd => 1, :dl_append => true }
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
      end

      params["DAEMON_LIST"] = daemon_list

      build_feature(feature_name, params, :verbosity => kwa[:verbosity])

      return schedd_names
    end


    def build_collector_feature(feature_name, kwa={})
      kwdef = { :verbosity => 0, :collector => 1, :portstart => 10000, :dl_append => true, :disable_plugins => true }
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
        if kwa[:disable_plugins] then
          params["COLLECTOR.%s.PLUGINS"%(locname)] = ""
        end
      end

      params["DAEMON_LIST"] = daemon_list

      build_feature(feature_name, params, :verbosity => kwa[:verbosity])

      return collector_names      
    end


    def build_accounting_group_feature(feature_name, group_tuples, kwa={})
      kwdef = { :verbosity => 0, :accept_surplus => false }
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

  end # module WallabyTools
end # module Albatross
