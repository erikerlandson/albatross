# cmd_build_feature.rb:  Build a feature in the wallaby store
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

$LOAD_PATH << "#{ENV['WALLABY_COMMAND_DIR']}"
require 'albatross_wallaby_tools.rb'

module Mrg
  module Grid
    module Config
      module Shell
        class BuildFeature < ::Mrg::Grid::Config::Shell::Command
          include ::Albatross::WallabyTools

          # opname returns the operation name; for "wallaby foo", it
          # would return "foo".
          def self.opname
            "build-feature"
          end
        
          # description returns a short description of this command, suitable 
          # for use in the output of "wallaby help commands".
          def self.description
            "Build a feature in the wallaby store"
          end
        
          def init_option_parser
            # Edit this method to generate a method that parses your command-line options.
            @params = {}
            @operation = 'replace'
            @feature_name = ''
            @verbosity = 0

            OptionParser.new do |opts|
              opts.banner = "Usage:  wallaby #{self.class.opname}\n#{self.class.description}"
        
              opts.on("-h", "--help", "displays this message") do
                puts @oparser
                exit
              end

              opts.on("-f", "--feature NAME", "feature name") do |name|
                @feature_name = name
              end

              opts.on("-p", "--param PARAM[,VAL]", Array, "parameter and value pair: may appear > once") do |pair|
                @params[pair[0]] = 0 if pair.length == 1
                @params[pair[0]] = pair[1] if pair.length == 2
              end

              opts.on("-o", "--operation OP", [:replace, :add, :remove], "feature editing option: def= %s" % [@operation]) do |op|
                @operation = case op when :replace; "replace" when :add; "add" when :remove; "remove" end
              end

              opts.on("-v", "--verbose", "verbose output") do
                @verbosity = 1
              end
            end
          end
        
          def act
            if @feature_name == "" then exit!(1, "wallaby #{self.class.opname}: missing --feature NAME") end
            build_feature(@feature_name, @params, :op => @operation, :verbosity => @verbosity)
            return 0
          end
        end
      end
    end
  end
end
