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
  # The WallabyTools module is designed to be mixed-in with a class that provides
  # a wallaby store variable named 'store', for example ::Mrg::Grid::Config::Shell::Command
  module WallabyTools

    def build_feature(feature_name, feature_params, kwargs={})
      kwdef = { :op => 'replace', :verbosity => 0 }
      kwargs = kwdef.merge(kwargs)

      puts "build_feature: %s" % feature_name if kwargs[:verbosity] > 0
      store.addFeature(feature_name) unless store.checkFeatureValidity([feature_name]) == []
      feature = store.getFeature(feature_name)

      store.checkParameterValidity(feature_params.keys).each do|param|
        puts "build_feature: declaring parameter %s" % param if kwargs[:verbosity] > 0
        store.addParam(param)
      end

      feature.modifyParams(kwargs[:op], feature_params)
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

  end # module WallabyTools
end # module Albatross
