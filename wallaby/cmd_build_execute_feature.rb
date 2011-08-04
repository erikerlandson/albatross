# cmd_build_execute_feature.rb:  Build an execute feature in the wallaby store
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
        class BuildExecuteFeature < ::Mrg::Grid::Config::Shell::Command
          include ::Albatross::WallabyTools

          # opname returns the operation name; for "wallaby foo", it
          # would return "foo".
          def self.opname
            "build-execute-feature"
          end
        
          # description returns a short description of this command, suitable 
          # for use in the output of "wallaby help commands".
          def self.description
            "Build an execute feature in the wallaby store"
          end
        
          def init_option_parser
            # Edit this method to generate a method that parses your command-line options.
            @feature_name = ''
            @verbosity = 0
            @nstartd = 1
            @nslots = 1
            @ndynamic = 0
            @dl_append = true
            @dedicated = true
            @preemption = false
            @ad_machine = false

            OptionParser.new do |opts|
              opts.banner = "Usage:  wallaby #{self.class.opname}\n#{self.class.description}"
        
              opts.on("-h", "--help", "displays this message") do
                puts @oparser
                exit
              end

              opts.on("-f", "--feature NAME", "feature name") do |name|
                @feature_name = name
              end

              opts.on("--nstartd N", Integer, "number of startds") do |n|
                @nstartd = n
              end

              opts.on("--nslots N", Integer, "number of slots per startd") do |n|
                @nslots = n
              end

              opts.on("--ndynamic N", Integer, "number of dynamic slots per slot") do |n|
                @ndynamic = n
              end

              opts.on("--[no-]dl-append", "append to daemon list") do |v|
                @dl_append = v
              end

              opts.on("--[no-]dedicated", "dedicated execute node") do |v|
                @dedicated = v
              end

              opts.on("--[no-]-preemption", "enable preemption") do |v|
                @preemption = v
              end

              opts.on("--[no-]ad-machine", "advertise machine name per startd") do |v|
                @ad_machine = v
              end

              opts.on("-v", "--verbose", "verbose output") do
                @verbosity = 1
              end
            end
          end
        
          def act
            if @feature_name == "" then exit!(1, "wallaby #{self.class.opname}: missing --feature NAME") end
            build_execute_feature(@feature_name, :verbosity => @verbosity, :startd => @nstartd, :slots => @nslots, :dynamic => @ndynamic,
                                  :dl_append => @dl_append, :dedicated => @dedicated, :preemption => @preemption, :ad_machine => @ad_machine)
            return 0
          end
        end
      end
    end
  end
end
