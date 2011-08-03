# Albatross::WallabyTools - Tools for abstracting wallaby object store operations
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

module Albatross
  module WallabyTools
    def build_feature(feature_name, feature_params, kwargs={})
      puts "build_feature: name= %s" % feature_name if kwargs[:verbosity] > 0
      kwdef = { :op => 'replace', :verbosity => 0 }
      kwargs = kwdef.merge(kwargs)
      store.addFeature(feature_name) unless store.checkFeatureValidity([feature_name]) == []
      feature = store.getFeature(feature_name)
      store.checkParameterValidity(feature_params.keys).each do|param|
        puts "build_feature: declaring parameter %s" % param if kwargs[:verbosity] > 0
        store.addParam(param)
      end
      feature.modifyParams(kwargs[:op], feature_params)
    end
  end # module WallabyTools
end # module Albatross
