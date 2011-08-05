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
              @parameter = "default"

              opts.banner = "Usage:  wallaby #{self.class.opname}\n#{self.class.description}"
        
              opts.on("-h", "--help", "displays this message") do
                puts @oparser
                exit
              end

              opts.on("-p", "--parameter [VAL]", "set param") do |v|
                @parameter = v
              end
            end
          end

          class UT < ::Test::Unit::TestCase
            include ::Albatross::WallabyTools
            include ::Albatross::WallabyUnitTestTools

            def setup
              puts "setting up"
              build_feature("EJE", {"P1" => "V1", "P2" => "V2"}, :verbosity => 1)
            end

            def test_1
              assert_equal([], store.checkFeatureValidity(["EJE"]))
            end

            def test_2
              assert_equal([], store.checkFeatureValidity(["DOES_NOT_EXIST"]))
            end

            def teardown
              puts "tearing down"
            end
          end
        
          class Base
            def foo
              puts "Base.foo"
            end
          end

          module Module
            def foo
              puts "Module.foo"
            end
          end

          class Derived < Base
            include Module
          end

          def act
            Derived.new.foo
            #exit!(0, "just testing!")
            UT.store=(store)
            ::Test::Unit::UI::Console::TestRunner.run(UT)
            return 0
          end
        end
      end
    end
  end
end
