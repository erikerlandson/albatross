# cmd_build_scheduler_feature.rb:  Build a scheduler feature in the wallaby store
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
        class BuildSchedulerFeature < ::Mrg::Grid::Config::Shell::Command
          include ::Albatross::WallabyTools

          # opname returns the operation name; for "wallaby foo", it
          # would return "foo".
          def self.opname
            "build-scheduler-feature"
          end
        
          # description returns a short description of this command, suitable 
          # for use in the output of "wallaby help commands".
          def self.description
            "Build a scheduler feature in the wallaby store"
          end
        
          def init_option_parser
            # Edit this method to generate a method that parses your command-line options.
            @feature_name = ''
            @verbosity = 0
            @nschedd = 1
            @dl_append = true

            OptionParser.new do |opts|
              opts.banner = "Usage:  wallaby #{self.class.opname}\n#{self.class.description}"
        
              opts.on("-h", "--help", "displays this message") do
                puts @oparser
                exit
              end

              opts.on("-f", "--feature NAME", "feature name") do |name|
                @feature_name = name
              end

              opts.on("--nschedd N", Integer, "number of schedds") do |n|
                @nschedd = n
              end

              opts.on("--[no-]dl-append", "append to daemon list") do |v|
                @dl_append = v
              end

              opts.on("-v", "--verbose", "verbose output") do
                @verbosity = 1
              end
            end
          end
        
          def act
            build_scheduler_feature(@feature_name, :verbosity => @verbosity, :schedd => @nschedd, :dl_append => @dl_append)
            return 0
          end
        end
      end
    end
  end
end
