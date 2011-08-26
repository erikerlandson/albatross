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

              @params[:nschedd] = 1
              opts.on("--nschedd N", Integer, "number of schedulers") do |v|
                @params[:nschedd] = v
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
            end

            def suite_teardown
              super # call super last, after test-specific teardown
            end

            def test_submit
              nodes = condor_nodes()
              log.info("pool nodes= %s" % [array_to_s(nodes)])

              candidate_nodes = select_nodes(nodes)
              log.info("candidate nodes= %s" % [array_to_s(candidate_nodes)])
              
              raise(Exception, "required %d target nodes, found only %d" % [params[:ntarget], candidate_nodes.length]) if candidate_nodes.length < params[:ntarget]
              target_nodes = candidate_nodes.first(params[:ntarget])

              log.info("target nodes= %s" % [array_to_s(target_nodes)])
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
