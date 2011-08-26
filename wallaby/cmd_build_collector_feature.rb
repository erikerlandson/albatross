# cmd_build_collector_feature.rb:  Build a collector feature in the wallaby store
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
        class BuildCollectorFeature < ::Mrg::Grid::Config::Shell::Command
          include ::Albatross::WallabyTools

          # opname returns the operation name; for "wallaby foo", it
          # would return "foo".
          def self.opname
            "build-collector-feature"
          end
        
          # description returns a short description of this command, suitable 
          # for use in the output of "wallaby help commands".
          def self.description
            "Build a collector feature in the wallaby store"
          end
        
          def init_option_parser
            @params = {}

            optp = OptionParser.new do |opts|
              opts.banner = "Usage:  wallaby #{self.class.opname} feature_name [options]\\n#{self.class.description}"
        
              opts.on("-h", "--help", "displays this message") do
                puts @oparser
                exit
              end

              @params[:collector] = 1
              opts.on("--ncollector N", Integer, "number of collectors: def= %s" % [@params[:collector]]) do |n|
                @params[:collector] = n
              end

              @params[:portstart] = 10000
              opts.on("--port-start N", Integer, "collector port start value: def= %s" % [@params[:portstart]]) do |n|
                @params[:portstart] = n
              end

              @params[:dl_append] = true
              opts.on("--[no-]dl-append", "append to daemon list: def= %s" % [@params[:dl_append]]) do |v|
                @params[:dl_append] = v
              end

              @params[:disable_plugins] = true
              opts.on("--[no-]disable-plugins", "disable collector plugins: def= %s" % [@params[:disable_plugins]]) do |v|
                @params[:disable_plugins] = v
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
            build_collector_feature(@params[:feature_name], @params)
            return 0
          end
        end
      end
    end
  end
end
