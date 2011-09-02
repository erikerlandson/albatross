# cmd_scale_test.rb:  condor scale test
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

require 'test/unit'
require 'test/unit/ui/console/testrunner'
  
$LOAD_PATH << "#{ENV['WALLABY_COMMAND_DIR']}"
require 'albatross_wallaby_tools.rb'


module Mrg
  module Grid
    module Config
      module Shell
        class ScaleTest < ::Mrg::Grid::Config::Shell::Command
          # opname returns the operation name; for "wallaby foo", it
          # would return "foo".
          def self.opname
            "scale-test"
          end
        
          # description returns a short description of this command, suitable 
          # for use in the output of "wallaby help commands".
          def self.description
            "condor scale test"
          end
        
          def init_option_parser
            # Edit this method to generate a method that parses your command-line options.
            @params = {}

            optp = OptionParser.new do |opts|
              
              opts.banner = "Usage:  wallaby #{self.class.opname}\n#{self.class.description}"
  
              opts.on("-h", "--help", "displays this message") do
                puts @oparser
                exit
              end

              @params[:ntarget] = 1
              opts.on("--ntarget N", Integer, "number of target machines") do |v|
                @params[:ntarget] = v
              end

              @params[:nstartd] = 1
              opts.on("--nstartd N", Integer, "number of startds per node") do |v|
                @params[:nstartd] = v
              end

              @params[:nslots] = 1
              opts.on("--nslots N", Integer, "number of slots per startd") do |v|
                @params[:nslots] = v
              end

              @params[:ndynamic] = 0
              opts.on("--ndynamic N", Integer, "number of dynamic slots per slot") do |v|
                @params[:ndynamic] = v
              end

              @params[:nsub] = 1
              opts.on("--nsub N", Integer, "number of submitters: def= %d" % [@params[:nsub]]) do |v|
                @params[:nsub] = v
              end

              @params[:duration] = 30
              opts.on("--duration N", Integer, "job duration (sec): def= %d" % [@params[:duration]]) do |v|
                @params[:duration] = v
              end

              @params[:nschedd] = 1
              opts.on("--nschedd N", Integer, "number of schedulers") do |v|
                @params[:nschedd] = v
              end

              @params[:sustain] = 60
              opts.on("--sustain N", Integer, "sustain submissions and or completions for N sec: def= %d" % [@params[:sustain]]) do |v|
                @params[:sustain] = v
              end

              opts.separator("\nsubmission rate testing")

              @params[:interval] = 1.0
              opts.on("--interval X", Float, "submit at interval X sec: def= %3.2f" % [@params[:interval]]) do |v|
                @params[:interval] = v
              end

              opts.separator("\ncompletion rate testing")

              @params[:njobs] = 1
              opts.on("--njobs N", Integer, "number of jobs for completion-rate: def= %d" % [@params[:njobs]]) do |v|
                @params[:njobs] = v
              end
            end

            ::Albatross::WallabyUnitTestTools.options(optp, @params)
          end

          class ScaleTest < ::Test::Unit::TestCase
            include ::Albatross::WallabyUnitTestTools
            include ::Albatross::WallabyTools
            include ::Albatross::CondorTools

            def suite_setup
              super # call super first, before test-specific setup

              raise(Exception, "Requested insufficient target nodes for %d schedulers" % [params[:nschedd]]) if params[:nschedd] > params[:ntarget]

              nodes = condor_nodes()
              log.info("pool nodes= %s" % [array_to_s(nodes)])

              candidate_nodes = select_nodes(nodes)
              log.info("candidate nodes= %s" % [array_to_s(candidate_nodes)])
              raise(Exception, "required %d target nodes, found only %d" % [params[:ntarget], candidate_nodes.length]) if candidate_nodes.length < params[:ntarget]

              @target_nodes = candidate_nodes.first(params[:ntarget])
              log.info("target nodes= %s" % [array_to_s(@target_nodes)])

              declare_features('GridScaleTest')
              build_access_feature('GridScaleTestAccess')
              
              @pslots, @dslots = build_execute_feature('GridScaleTestExecute', :startd => params[:nstartd], :slots => params[:nslots], :dynamic => params[:ndynamic], :dl_append => false)

              build_feature('GridScaleTestUpdate', {"UPDATE_INTERVAL" => "60"})
              build_feature('GridScaleTestPorts', {"LOWPORT" => "1024", "HIGHPORT" => "64000"})
              
              declare_groups('GridScaleTest')
              set_group_features('GridScaleTest', ['GridScaleTestExecute', 'GridScaleTestPorts', 'GridScaleTestUpdate', 'GridScaleTestAccess', 'GridScaleTest', 'Master', 'NodeAccess'])
              
              # set up execute and other config on target nodes
              clear_nodes(@target_nodes)
              set_node_groups(@target_nodes, 'GridScaleTest')

              # get history files
              build_feature('GridScaleTestFetch', {"ALLOW_ADMINISTRATOR" => (">= %s"%[fq_hostname]), "MAX_HISTORY_LOG" => "1000000000"})

              # set up scheduler features as needed on target nodes
              # schedd names are same as node names they run on, by default
              @schedd_names = @target_nodes.first(params[:nschedd])
              set_node_features(@schedd_names, ['GridScaleTestFetch', 'Scheduler'])
              
              # turn off plugins
              build_feature('GridScaleTestNoPlugins', {"MASTER.PLUGINS" => "", "SCHEDD.PLUGINS" => "", "COLLECTOR.PLUGINS" => "", "NEGOTIATOR.PLUGINS" => "", "STARTD.PLUGINS" => ""})
              # turn off preemption
              build_feature('GridScaleTestNoPreempt', {"NEGOTIATOR_CONSIDER_PREEMPTION" => "FALSE", "PREEMPTION_REQUIREMENTS" => "FALSE", "RANK" => "0", "SHADOW_TIMEOUT_MULTIPLIER" => "4", "SHADOW_WORKLIFE" => "36000"})

              # miscellaneous settings
              build_feature('GridScaleTestNeg', {"NEGOTIATOR_INTERVAL" => "30", "NEGOTIATOR_MAX_TIME_PER_SUBMITTER" => "31536000", "NEGOTIATOR_DEBUG" => "", "MAX_NEGOTIATOR_LOG" => "100000000", "SCHEDD_DEBUG" => "", "MAX_SCHEDD_LOG" => "100000000", "COLLECTOR_DEBUG" => "", "SHADOW_LOCK" => "", "SHADOW_LOG" => "", "NEGOTIATOR_PRE_JOB_RANK" => "0", "NEGOTIATOR_POST_JOB_RANK" => "0"})

              set_node_features(params[:condor_host], ['GridScaleTestFetch', 'GridScaleTestNeg', 'GridScaleTestNoPreempt', 'GridScaleTestPorts'], :op => 'insert')

              take_snapshot("grid_scale_test_%s" % [@test_date])
              store.activateConfiguration(_timeout=60)
              
              poll_for_slots(params[:ntarget]*@pslots, :group => 'GridScaleTest', :interval => 30, :maxtime => 300, :expected => @target_nodes)
            end

            def suite_teardown
              super # call super last, after test-specific teardown
            end

            def test_01_complete_rate
              start = Time.now.to_i

              jobs_per = 1+Integer(Float(params[:njobs])/Float(@schedd_names.length))
              pidlist = []
              j = 0
              @schedd_names.each do |schedd|
                cjscmd = "cjs -shell -dir '%s' -duration %d -n %d -sub %d -remote '%s' -reqs 'stringListMember(\"GridScaleTest\", WallabyGroups) && (TARGET.Arch =!= UNDEFINED) && (TARGET.OpSys =!= UNDEFINED) && (TARGET.Disk >= 0) && (TARGET.Memory >= 0) && (TARGET.FileSystemDomain =!= UNDEFINED)' -append '+AlbatrossTestTag=\"ScaleTest\"' -append '+LeaveJobInQueue=False' >'%s/sh_cr_out%03d' 2>'%s/sh_cr_err%03d'" % [@tmpdir, params[:duration], jobs_per, params[:nsub], schedd, @tmpdir, j, @tmpdir, j]

                log.debug("cjscmd= %s" % [cjscmd])
                pid = IO.popen(cjscmd).pid
                Process.detach(pid)
                pidlist.push(pid)
                j += 1
              end

              log.info("Waiting for %d submission processes to complete..." % [pidlist.length])
              poll_for_process_completion(pidlist)

              log.info("pausing for jobs to queue up")
              sleep(15)
              poll_for_empty_job_queue(:schedd => @schedd_names, :tag => "ScaleTest", :interval => 60, :maxtime => 120+params[:sustain], :remove_jobs => params[:sustain])

              hfname = "%s/cr_history" % [@tmpdir]
              collect_history(:nodes => @schedd_names, :wdir => @tmpdir, :fname => hfname)

              collect_rates(hfname, :odir => @tmpdir, :since => start)
            end

            def test_02_submit_rate
              start = Time.now.to_i
              log.info("spawning %d submit processes..." % [params[:nsub]])
              submit_procs = []
              params[:nsub].times do |j|
                schedd_name = @schedd_names[j % params[:nschedd]]
                cjscmd = "cjs -shell -dir '%s' -duration %d -xgroups U%03d 1 -reqs 'stringListMember(\"GridScaleTest\", WallabyGroups) && (TARGET.Arch =!= UNDEFINED) && (TARGET.OpSys =!= UNDEFINED) && (TARGET.Disk >= 0) && (TARGET.Memory >= 0) && (TARGET.FileSystemDomain =!= UNDEFINED)' -ss -ss-interval %f -ss-maxtime %d -append '+AlbatrossTestTag=\"ScaleTest\"' -append '+LeaveJobInQueue=False' -remote '%s' >'%s/sh_out%03d' 2>'%s/sh_err%03d'" % [@tmpdir, params[:duration], j, params[:interval], params[:sustain], schedd_name, @tmpdir, j, @tmpdir, j]
                log.debug("cjscmd= %s" % [cjscmd])
                pid = IO.popen(cjscmd).pid
                Process.detach(pid)
                submit_procs.push(pid)
              end

              log.info("Waiting for %d submission processes to complete..." % [params[:nsub]])

              elapsed = poll_for_process_completion(submit_procs)

              njobs = job_count(:tag => "ScaleTest", :schedd => @schedd_names)
              log.info("elapsed time= %f  njobs= %d  sustained rate= %f" % [elapsed, njobs, njobs / elapsed])

              poll_for_empty_job_queue(:schedd => @schedd_names, :tag => "ScaleTest", :interval => 60, :maxtime => 120+params[:sustain], :remove_jobs => params[:sustain])

              hfname = "%s/sr_history" % [@tmpdir]
              collect_history(:nodes => @schedd_names, :wdir => @tmpdir, :fname => hfname)

              collect_rates(hfname, :odir => @tmpdir, :since => start, :srates => true, :crates => true)
            end
          end
        
          def act
            ScaleTest.store=(store)
            ScaleTest.params=(@params)

            ::Test::Unit::UI::Console::TestRunner.run(ScaleTest)

            return 0
          end
        end
      end
    end
  end
end
