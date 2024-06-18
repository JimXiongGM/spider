# Copyright 2017 Google Inc.
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
"""
seq2seq library base module
"""

from __future__ import absolute_import, division, print_function

from seq2seq import (
    contrib,
    data,
    decoders,
    encoders,
    global_vars,
    graph_utils,
    inference,
    losses,
    metrics,
    models,
    test,
    training,
)
from seq2seq.graph_module import GraphModule
