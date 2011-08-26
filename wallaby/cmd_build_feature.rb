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

            optp = OptionParser.new do |opts|
              opts.banner = "Usage:  wallaby #{self.class.opname} feature_name [options]\n#{self.class.description}"
        
              opts.on("-h", "--help", "displays this message") do
                puts @oparser
                exit
              end

              @params[:params] = {}
              opts.on("-p", "--param PARAM[,VAL]", Array, "parameter and value pair: may appear > once") do |pair|
                @params[:params][pair[0]] = 0 if pair.length == 1
                @params[:params][pair[0]] = pair[1] if pair.length == 2
              end

              @params[:operation] = 'replace'
              opts.on("-o", "--operation OP", [:replace, :add, :remove], "feature editing option: def= %s" % [@params[:operation]]) do |op|
                @params[:operation] = op.to_s
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
            build_feature(@params[:feature_name], @params[:params], :op => @params[:operation])
            return 0
          end
        end
      end
    end
  end
end
