# cmd_build_accounting_group_feature.rb:  Build an accounting group feature in the wallaby store
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
        class BuildAccountingGroupFeature < ::Mrg::Grid::Config::Shell::Command
          include ::Albatross::WallabyTools

          # opname returns the operation name; for "wallaby foo", it
          # would return "foo".
          def self.opname
            "build-accounting-group-feature"
          end
        
          # description returns a short description of this command, suitable 
          # for use in the output of "wallaby help commands".
          def self.description
            "Build an accounting group feature in the wallaby store"
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

              @params[:group_tuples] = []
              opts.on("-g", "--group NAME,QUOTA[,ACCEPT_SURPLUS[,IS_STATIC]]", Array, "group tuple: may appear > once") do |t|
                accept_surplus = if t.length >= 3 then if t[2]=="true" then true else false end else nil end
                is_static = if t.length >= 4 then if t[3]=="true" then true else false end else nil end
                @params[:group_tuples] += [ [ t[0], t[1].to_f, accept_surplus, is_static ] ]
              end

              @params[:accept_surplus] = false
              opts.on("--[no-]accept-surplus", "set default accept surplus: def= %s" % [@params[:accept_surplus]]) do |v|
                @params[:accept_surplus] = v
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
            build_accounting_group_feature(@params[:feature_name], @params[:group_tuples], @params)
            return 0
          end
        end
      end
    end
  end
end
