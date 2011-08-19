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
            end

            ::Albatross::WallabyUnitTestTools.options(optp, @params)
          end

          class ScaleTest < ::Test::Unit::TestCase
            include ::Albatross::WallabyUnitTestTools
            include ::Albatross::WallabyTools
            include ::Albatross::CondorTools

            def suite_setup
              # call super first
              super
            end

            def suite_teardown
              # call super last
              super
            end

            def test_submit
              puts "verbosity= " + params[:verbosity].to_s
              
              puts "td= " + @test_date

              nodes = condor_nodes(:verbosity => 1)
              puts "nodes= %s" % [nodes.join(" ")]

              nodes = select_nodes(nodes, :checkin_since => 0, :verbosity => 1)
              puts "nodes= %s" % [nodes.join(" ")]
              

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
