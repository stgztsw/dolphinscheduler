<template>
  <div class="conTop">
    <div class="input-text" style="position: fixed;top:50%;left:50%;transform: translate(-50%,-50%); width: 50%">

      <div class="container" style="position: relative;left: 35%;text-align:center" align="center">
        <b><i class="el-icon-search" style="font-size: 24px;"></i></b>
        <b><h2>全 局 搜 索</h2></b>
      </div>
      <br/>

      <el-input placeholder="请输入内容" v-model="searchParams.searchVal" class="input-with-select" @keyup.enter.native="_onQuery">
        <el-select v-model="select" slot="prepend" placeholder="请选择">
          <el-option label="工作流实例" value="1"></el-option>
          <!--          <el-option label="Task实例" value="2"></el-option>-->
        </el-select>
        <el-button type="success" slot="append" icon="el-icon-search" @click="_onQuery"></el-button>

      </el-input>
      <br/><br/>
      <h5 style="position: absolute;width:60px;">高级选项 </h5>
      <el-switch slot="append"
                 v-model="moreCondition"
                 active-color="#13ce66"
                 inactive-color="#ff4949"
                 @click="_conditionButton"
                 style="position: absolute;left:70px;margin-left:5px;">
      </el-switch>
      <br/><br/>
      <div v-show="moreCondition">
        <h5 style="position: absolute;width:60px;">执行状态 </h5>
        <el-select
          v-model="stateList"
          placeholder="不限"
          multiple
          collapse-tags
          clearable
          class="tags_select_input"
          style="position: absolute;left:70px;margin-left:5px;width: 500px">
          <el-option
            v-for="city in stateTypeList"
            :key="city.label"
            :value="city.code"
            :label="city.label">
            　　　　　<!-- 单行文字溢出显示省略号 -->
            　　　　　<div style="width:300px;overflow:hidden;text-overflow: ellipsis;display:inline-block">{{city.label}}</div>
            　　　　　<!-- 当下拉多选时如果不加上display:inline-block,选中当前下拉时后面选中那个“√”将无法显示 -->
            　　　　　<!-- 单行情况下如果文字换行显示，并未隐藏，需加上：white-space: nowrap; -->
            　　　　　<!-- 多行行文字溢出显示省略号需加上：display: -webkit-box; -webkit-box-orient: vertical;-webkit-line-clamp: 4; -->

            　　　　　<!-- 溢出文字隐藏后将无法查看文本全部内容，此时可以使用el-tooltip组件 -->
            　　　　　<!-- 还有个问题就是虽然这溢出的文本隐藏了，并且现在鼠标指上去也能看见全部内容了，但是现在就算鼠标指到“未溢出的文本”也会有这个效果(这不是我想要的效果)，所以加了个v-if来控制 -->
            　　　　　<!-- v-if的长度空根据下拉框的宽度来进行调整 -->
            　　　　　<el-tooltip class="item" effect="dark" :content="city.label" placement="top-start" v-if="city.label.length >= 10">
            　　　　　　<div style="width:70px;overflow:hidden;text-overflow: ellipsis;display:inline-block">{{city.label}}</div>
            　　　　　</el-tooltip>
            　　　　</el-option>
        </el-select>
        <br/><br/><br/>
        <h5 style="position: absolute;width:90px;">选择日期区间 </h5>
        <div style="position: absolute;left:100px;margin-left:5px;width: 500px">
          <x-datepicker
            ref="datepicker"
            @on-change="_onChangeStartStop"
            type="daterange"
            format="YYYY-MM-DD HH:mm:ss"
            placement="bottom-end"
            :value="[searchParams.globalStartDate,searchParams.globalEndDate]"
            :panelNum="2">
            <x-input slot="input" readonly slot-scope="{value}" :value="value" style="width: 310px;" size="small" :placeholder="$t('Select date range')">
              <em slot="suffix"
                  @click.stop="_dateEmpty()"
                  class="ans-icon-fail-solid"
                  v-show="value"
                  style="font-size: 13px;cursor: pointer;margin-top: 1px;">
              </em>
            </x-input>
          </x-datepicker>
        </div>
      </div>


    </div>

  </div>
</template>

<script>
  import {mapActions} from "vuex";
  import { stateType } from '../_source/instanceConditions/common'
  import { setUrlParams } from '@/module/util/routerUtil'

  export default {

    data() {
      return {
        moreCondition: false,
        // state(list)
        stateTypeList: stateType,
        stateList: [],
        searchParams :{
          // Search keywords
          searchVal: '',
          // Number of pages
          pageSize: 10,
          // Current page
          pageNo: 1,
          // host
          host: '',
          // State
          stateType: '',
          // Start Time
          globalStartDate: '',
          // End Time
          globalEndDate: '',
          // Exectuor Name
          executorName: '',
        },
        // loading
        isLoading: true,
        // total
        total: null,
        // data
        processInstanceList: [],
        // 被绑定的选择值
        select: '1',
      }
    },
    methods: {
      ...mapActions('dag', ['getGlobalProcessInstance']),
      /**
       * empty date
       */
      _dateEmpty () {
        this.searchParams.globalStartDate = ''
        this.searchParams.globalEndDate = ''
        this.$refs.datepicker.empty()
      },
      /**
       * change times
       */
      _onChangeStartStop (val) {
        this.searchParams.globalStartDate = val[0]
        this.searchParams.globalEndDate = val[1]
      },
      _conditionButton() {
        this.moreCondition = !this.moreCondition;
        if (!this.moreCondition){
          this.searchParams.state=''
          this.searchParams.globalStartDate=''
          this.searchParams.globalEndDate=''
        }
      },
      _onQuery() {
        console.log("enter event")
        this.searchParams.pageNo = 1
        this.searchParams.stateType = JSON.stringify(this.stateList)
        // this.searchParams.stateType = this.stateList
        // setUrlParams(this.searchParams)
        if (this.select==='1') {
          this._getProcessInstanceListP()
        } else if (this.select==='2') {
          this._getTaskInstanceListP()
        }
      },
      /**
       * get list data
       */
      _getProcessInstanceListP() {
        console.log("_getProcessInstanceListP")
        this.getGlobalProcessInstance(this.searchParams).then(res => {
          if (this.searchParams.pageNo > 1 && res.totalList.length == 0) {
            this.searchParams.pageNo = this.searchParams.pageNo - 1
            console.log(this.processInstanceList.length>0)
            if (this.processInstanceList.length>0){
              this.$router.push({
                name:'projects-instance-global-list',
                params:{
                  processInstanceList:this.processInstanceList,
                  searchParams: this.searchParams,
                  // total:this.total
                }
              })
            }
          } else {
            console.log(res.totalList.length)
            this.processInstanceList = []
            this.processInstanceList = res.totalList
            this.total = res.total
            this.isLoading = false
            const that = this;
            console.log(that.processInstanceList.length>0)
            if (that.processInstanceList.length>0){
              that.$router.push({
                name:'projects-instance-global-list',
                params:{
                  processInstanceList:that.processInstanceList,
                  searchParams: that.searchParams,
                  // total:this.total
                }
              })
            }
          }
        }).catch(e => {
          this.isLoading = false;
          console.log("query error:",e)
          this.$message.error(e.msg || '');
        })
      },
      _getTaskInstanceListP() {
        // pass
      },
      selectAll() {
        if (this.stateList.length < this.stateTypeList.length) {
          this.stateList = []
          this.stateTypeList.map((item) => {
            this.stateList.push(item.name)
          })
          this.stateList.unshift('全选')
        } else {
          this.stateList = []
        }
      },
      changeSelect(val) {
        if (!val.includes('全选') && val.length === this.stateTypeList.length) {
          this.stateList.unshift('全选')
        } else if (val.includes('全选') && (val.length - 1) < this.stateTypeList.length) {
          this.stateList = this.stateList.filter((item) => {
            return item !== '全选'
          })
        }
      },
      removeTag(val) {
        if (val === '全选') {
          this.stateList = []
        }
      }
    }
  }
</script>

<style>
  body {
    /*background-image: url("./images/backgroud.jpeg");*/
  }
  .el-select .el-input {
    width: 130px;
  }
  .input-with-select .el-input-group__prepend {
    background-color: #fff;
  }
  .container {
    display: flex;
    align-items: center;
  }

  .container i {
    margin-right: 10px;
  }
  .tags_select_input /deep/ .el-select__tags {
    // height: 40px;
      white-space: nowrap;
      overflow: hidden;
      flex-wrap: nowrap;
    }
  .tags_select_input /deep/ .el-select__tags-text {
    display: inline-block;
    max-width: 300px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .tags_select_input /deep/ .el-tag__close.el-icon-close {
    top: -6px;
    right: -4px;
  }
</style>





