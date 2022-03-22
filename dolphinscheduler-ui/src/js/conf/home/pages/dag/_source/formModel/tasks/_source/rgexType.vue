/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
<template>
  <div class="rgex-type-model">
    <x-select
      v-model="rgexTypeId"
      :disabled="isDetails"
      @on-change="_handleRgexTypeChanged"
      style="width: 120px;">
      <x-option
        v-for="city in rgexTypeList"
        :key="city.id"
        :value="city.id"
        :label="city.code">
      </x-option>
    </x-select>
  </div>
</template>
<script>
  import _ from 'lodash'
  import { rgexTypeList } from './commcon'
  import disabledState from '@/module/mixin/disabledState'
  export default {
    name: 'rgex-type',
    data () {
      return {
        // sql(List)
        rgexTypeList: rgexTypeList,
        // sql
        rgexTypeId: '0'
      }
    },
    mixins: [disabledState],
    props: {
      rgexType: String
    },
    methods: {
      /**
       * return sqlType
       */
      _handleRgexTypeChanged (val) {
        this.$emit('on-rgexType', val.value)
      }
    },
    watch: {
    },
    created () {
      this.$nextTick(() => {
        if (this.rgexType != 0) {
          this.rgexTypeId = this.rgexType
        } else {
          this.rgexTypeId = this.rgexTypeList[0].id
        }
      })
    },
    mounted () {
    }
  }
</script>
