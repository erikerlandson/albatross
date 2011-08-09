# cmd_unit_test.rb:  unit test study
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
        class UnitTest < ::Mrg::Grid::Config::Shell::Command
          # opname returns the operation name; for "wallaby foo", it
          # would return "foo".
          def self.opname
            "unit-test"
          end
        
          # description returns a short description of this command, suitable 
          # for use in the output of "wallaby help commands".
          def self.description
            "unit test study"
          end
        
          def init_option_parser
            # Edit this method to generate a method that parses your command-line options.
            OptionParser.new do |opts|
              @params = {}
              @params[:p] = "default"

              opts.banner = "Usage:  wallaby #{self.class.opname}\n#{self.class.description}"
        
              opts.on("-h", "--help", "displays this message") do
                puts @oparser
                exit
              end

              opts.on("-p", "--parameter [VAL]", "set param") do |v|
                @params[:p] = v
              end
            end
          end

          class UT < ::Test::Unit::TestCase
            include ::Albatross::WallabyTools
            include ::Albatross::WallabyUnitTestTools

            # optional -- supported by WallabyUnitTestTools
            def suite_setup
              puts "suite_setup"
              build_feature("EJE", {"P1" => "V1", "P2" => "V2"}, :verbosity => 1)
              @suite_state = "set"
            end

            # optional -- supported by WallabyUnitTestTools
            def suite_teardown
              puts "suite_teardown"
            end

            # optional -- Test::Unit standard
            def setup
              puts "setup"
            end

            # optional -- Test::Unit standard
            def teardown
              puts "teardown"
            end

            def test_1
              # a test for feature constructed in suite_setup - should pass
              assert_equal([], store.checkFeatureValidity(["EJE"]))
            end

            def test_2
              # this test should fail
              assert_equal([], store.checkFeatureValidity(["DOES_NOT_EXIST"]))
            end

            def test_3
              # test params set from params=
              assert_equal("eje", params[:p])
            end

            def test_4
              # test object state set in suite_setup
              assert_equal("set", @suite_state)
            end
          end
        
          def act
            UT.store=(store)
            UT.params=(@params)
            ::Test::Unit::UI::Console::TestRunner.run(UT)
            return 0
          end
        end
      end
    end
  end
end
