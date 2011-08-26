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
            @params = {}

            optp = OptionParser.new do |opts|
              opts.banner = "Usage:  wallaby #{self.class.opname} feature_name [options]\n#{self.class.description}"
        
              opts.on("-h", "--help", "displays this message") do
                puts @oparser
                exit
              end

              @params[:startd] = 1
              opts.on("--nstartd N", Integer, "number of startds: def= %s" % [@params[:startd]]) do |n|
                @params[:startd] = n
              end

              @params[:slots] = 1
              opts.on("--nslots N", Integer, "number of slots per startd: def= %s" % [@params[:slots]]) do |n|
                @params[:slots] = n
              end

              @params[:dynamic] = 0
              opts.on("--ndynamic N", Integer, "number of dynamic slots per slot: def= %s" % [@params[:dynamic]]) do |n|
                @params[:dynamic] = n
              end

              @params[:dl_append] = true
              opts.on("--[no-]dl-append", "append to daemon list: def= %s" % [@params[:dl_append]]) do |v|
                @params[:dl_append] = v
              end

              @params[:dedicated] = true
              opts.on("--[no-]dedicated", "dedicated execute node: def= %s" % [@params[:dedicated]]) do |v|
                @params[:dedicated] = v
              end

              @params[:preemption] = false
              opts.on("--[no-]preemption", "enable preemption: def= %s" % [@params[:preemption]]) do |v|
                @params[:preemption] = v
              end

              @params[:ad_machine] = false
              opts.on("--[no-]ad-machine", "advertise machine name per startd: def= %s" % [@params[:ad_machine]]) do |v|
                @params[:ad_machine] = v
              end
            end

            ::Albatross::LogUtils.options(optp, @params)
          end
        
          def positional_args(*args)
            (puts @oparser; exit) if (args).length < 1
            @params[:feature_name] = args[0]
          end
          register_callback(:after_option_parsing, :positional_args)
          
          def act
            self.class.params=(@params)
            build_execute_feature(@params[:feature_name], @params)
            return 0
          end
        end
      end
    end
  end
end
