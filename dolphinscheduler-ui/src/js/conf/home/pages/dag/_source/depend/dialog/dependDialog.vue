<template>
  <div class="dialog" v-show="showMask">
    <div class="dialog-container">
      <div class="dialog-title">{{title}}</div>
      <div class="content" >
        <div v-if="!(Object.prototype.isPrototypeOf($store.state.dag.dependList.parents) && Object.keys($store.state.dag.dependList.parents).length === 0)">

        </div>
        <div v-if="$store.state.dag.dependList.name!==''">
          <h5>当前节点：</h5>
          <x-button type="primary" v-html="currentStr" size="small" shape="Plugin" long>Default</x-button>
        </div>
        <div>
          <h5>上一层依赖：</h5>
          <el-table
            :data="parentStrArray"
            style="width: 100%"
            :row-style="{height: '30px'}"
            tooltip-effect="dark"
            max-height="300"
            highlight-current-row="true"
            show-overflow-tooltip="true"
            empty-text="不存在上游依赖"
            @row-click="clickData">
            <el-table-column
              prop="name"
              label="workName"
              width="300">
            </el-table-column>
            <el-table-column
              prop="state"
              label="状态"
              width="180">
            </el-table-column>
            <el-table-column
              prop="id"
              label="ID"
              width="180">
              <templete slot-scope="scope">
                <span v-if="workType=='instance'">
                  {{scope.row.processId}}
                </span>
                <span v-if="workType==='definition'">
                  {{scope.row.definitionId}}
                </span>
              </templete>
            </el-table-column>
            <el-table-column
              prop="dependType"
              label="依赖类型"
              width="180">
            </el-table-column>
            <el-table-column
              fixed="right"
              label="操作"
              width="120">
              <template slot-scope="scope">
                <el-button
                  @click="jumpParentWorkFlow(scope.$index)"
                  type="primary" plain
                  size="small">
                  <b v-if="workType==='instance'">跳转实例</b>
                  <b v-if="workType==='definition'">跳转工作流</b>
                </el-button>
              </template>
            </el-table-column>
          </el-table>
        </div>
        <div>
          <br/>
        </div>
        <div>
          <h5>下一层依赖：</h5>
          <el-table
            :data="childStrArray"
            style="width: 100%"
            :row-style="{height: '30px'}"
            tooltip-effect="dark"
            max-height="300"
            highlight-current-row="true"
            show-overflow-tooltip="true"
            empty-text="不存在下游依赖"
            @row-click="clickData">
            <el-table-column
              prop="name"
              label="workName"
              width="300">
            </el-table-column>
            <el-table-column
              prop="state"
              label="状态"
              width="180">
            </el-table-column>
            <el-table-column
              prop="id"
              label="ID"
              width="180">
              <templete slot-scope="scope">
                <span v-if="workType=='instance'">
                  {{scope.row.processId}}
                </span>
                <span v-if="workType==='definition'">
                  {{scope.row.definitionId}}
                </span>
              </templete>
            </el-table-column>
            <el-table-column
              prop="dependType"
              label="依赖类型"
              width="180">
            </el-table-column>
<!--            <el-table-column v-if="false" prop="processId" label="processId" width="0"></el-table-column>-->
<!--            <el-table-column v-if="false" prop="definitionId" label="definitionId" width="0"></el-table-column>-->
            <el-table-column
              fixed="right"
              label="操作"
              width="120">
              <template slot-scope="scope">
                <el-button
                  @click="jumpChildWorkFlow(scope.$index)"
                  type="primary" plain
                  size="small">
                  <b v-if="workType==='instance'">跳转实例</b>
                  <b v-if="workType==='definition'">跳转工作流</b>
                </el-button>
              </template>
            </el-table-column>
          </el-table>
        </div>
      </div>
      <div class="btns">
        <div v-if="type != 'confirm'" class="default-btn" @click="closeBtn">
          {{cancelText}}
        </div>
<!--        <div v-if="type == 'danger'" class="danger-btn" @click="dangerBtn">-->
<!--          {{dangerText}}-->
<!--        </div>-->
<!--        <div v-if="type == 'confirm'" class="confirm-btn" @click="confirmBtn">-->
<!--          {{confirmText}}-->
<!--        </div>-->
      </div>
      <div class="close-btn" @click="closeMask"><i class="iconfont icon-close"></i></div>
    </div>

  </div>
</template>
<script>
  import {mapActions, mapMutations} from "vuex";

  export default {
    props: {
      id:{
        type: Number,
        default: ''
      },
      relation:{
        type: String,
        default: 'ONE_ALL'
      },
      sendVal: false,
      value: {},
      // 类型包括 defalut 默认， danger 危险， confirm 确认，
      type:{
        type: String,
        default: 'default'
      },
      content: {
        type: String,
        default: ''
      },
      title: {
        type: String,
        default: ''
      },
      workType: {
        type: String,
        default: ''
      },
      cancelText: {
        type: String,
        default: '取消'
      },
      dangerText: {
        type: String,
        default: '删除'
      },
      confirmText: {
        type: String,
        default: '确认'
      },
    },
    data(){
      return{
        idListState: [],
        dependList: [],
        showMask: false,
        currentStr: '',
        parentStrArray: [],
        childStrArray: [],
        tableHeaders: [
          { label: 'workName', prop: 'name' },
          { label: '状态', prop: 'state' },
          { label: 'ID', prop: 'processId' },
          { label: '依赖类型', prop: 'dependType'}
        ],
        isInstance: false
      }
    },
    methods:{
      ...mapMutations('dag', ['setIdListState','setDependList']),
      ...mapActions('dag', ['getDependView',"getDependObject"]),
      getColumnId(processId,definitionId){
        return this.workType==="instance"?processId:definitionId
      },
      clickData(){
        console.log("点击了当前行")
      },
      // dialog methods
      closeMask(){
        this.showMask = false;
      },

      jumpParentWorkFlow(index){
        console.log(" in jumpParentWorkFlow",index)

        if (this.workType==='instance') {
          this.$router.push({ path: `/projects/instance/list/${this.parentStrArray[index].processId}` })
        } else if (this.workType==='definition') {
          this.$router.push({ path: `/projects/definition/list/${this.parentStrArray[index].definitionId}` })
        }
      },
      jumpChildWorkFlow(index){
        console.log(" in jumpChildWorkFlow",index)

        if (this.workType==='instance') {
          this.$router.push({ path: `/projects/instance/list/${this.childStrArray[index].processId}` })
        } else if (this.workType==='definition') {
          this.$router.push({ path: `/projects/definition/list/${this.childStrArray[index].definitionId}` })
        }
      },
      closeBtn(){
        this.$emit('cancel');
        this.closeMask();
        this.sendVal = false;
      },
      dangerBtn(){
        this.$emit('danger');
        this.closeMask();
      },
      confirmBtn(){
        this.$emit('confirm');
        this.closeMask();
      },
      // data load methods
      init() {
        const data = this.handleData()
        console.log("进入init")
        // this.content = this.dependList.name
        // console.log("..................",data.processId,data.name)
      },
      // 拿到depend的对象
      handleData() {
        let _this = this;
        new Promise((resolve, reject) => {
          // Process instance details

          _this.getDependView({id: _this.id, relation: _this.relation, workType:_this.workType})
            .then((data) => {
              let item = data
              console.log(item)
              // _this.structureDependArray(item)
              const d = {
                processId: item.processId,
                definitionId: item.definitionId,
                treeType: item.treeType,
                name: item.name,
                state: item.state,
                parents: item.parents,
                childs: item.childs
              };
              _this.setDependList(d)
              _this.dependList = d
              // _this.$store.state["dag/dependList"]
              _this.getParentsLoop(_this)
              _this.getChildsLoop(_this)

              _this.currentStr =
                "Name: "+ _this.$store.state.dag.dependList.name+
                " ,状态: "+(_this.workType==="instance"?
                _this.$store.state.dag.dependList.state:"online")+
                " ,ID: "+(_this.workType==="instance"?
                  _this.$store.state.dag.dependList.processId:_this.$store.state.dag.dependList.definitionId)

              resolve(data)
            }).catch((e) => {
            _this.isLoading = false
            console.log("faild===", e)
          })
        })
      },
      getParentsLoop(_this) {
        const obj = _this.$store.state.dag.dependList.parents
        if (!(Object.prototype.isPrototypeOf(obj) && Object.keys(obj).length === 0)) {
          _this.parentStrArray = []
          console.log("进到了 getParentsLoop 循环")
          for (var i=0;i<obj.length;i++){
            let pArr =
            [{
              name:obj[i].name,
              state:obj[i].state,
              processId:obj[i].processId,
              definitionId:obj[i].definitionId,
              dependType:obj[i].dependType
            }]
            let pObj = {
              name:obj[i].name,
              state:obj[i].state,
              processId:obj[i].processId,
              definitionId:obj[i].definitionId,
              dependType:obj[i].dependType
            }
            // let concat = _this.parentStrArray.concat(pArr);
            // console.log(" add concat",concat)
            _this.parentStrArray.push(pObj)
          }
        }
      },
      getChildsLoop(_this) {
        const obj = _this.$store.state.dag.dependList.childs
        if (!(Object.prototype.isPrototypeOf(obj) && Object.keys(obj).length === 0)) {
          _this.childStrArray = []
          for (var i=0;i<obj.length;i++){
            let pArr =
              [{
                name:obj[i].name,
                state:obj[i].state,
                processId:obj[i].processId,
                definitionId:obj[i].definitionId,
                dependType:obj[i].dependType
              }]
            let pObj = {
              name:obj[i].name,
              state:obj[i].state,
              processId:obj[i].processId,
              definitionId:obj[i].definitionId,
              dependType:obj[i].dependType
            }
            // _this.childStrArray.concat(pArr)
            _this.childStrArray.push(pObj)
          }
        }
      }

    },
    mounted(){
      this.showMask = this.value;
    },
    watch:{
      value(newVal, oldVal){
        this.showMask = newVal;
      },
      showMask(val) {
        this.$emit('input', val);
      }
    },
    created() {
      console.log(this.id,this.relation,this.workType)
      this.init()
    }
  }
</script>
<style lang="scss" scoped>
  .dialog{
    position: fixed;
    top: 0;
    bottom: 0;
    left: 0;
    right: 0;
    background: rgba(0, 0, 0, 0.6);
    z-index: 9999;
    .dialog-container{
      width: 1000px;
      height: 760px;
      background: #ffffff;
      position: absolute;
      top: 50%;
      left: 50%;
      transform: translate(-50%, -50%);
      border-radius: 8px;
      position: relative;
      .dialog-title{
        width: 100%;
        height: 60px;
        font-size: 18px;
        color: #696969;
        font-weight: 600;
        padding: 16px 50px 0 20px;
        box-sizing: border-box;
      }
      .content{
        color: #797979;
        line-height: 26px;
        padding: 0 20px;
        box-sizing: border-box;
      }
      .inp{
        margin: 10px 0 0 20px;
        width: 200px;
        height: 40px;
        padding-left: 4px;
        border-radius: 4px;
        border: none;
        background: #efefef;
        outline: none;
        &:focus{
          border: 1px solid #509EE3;
        }
      }
      .btns{
        width: 100%;
        height: 60px;
        // line-height: 60px;
        position: absolute;
        bottom: 0;
        left: 0;
        text-align: right;
        padding: 0 16px;
        box-sizing: border-box;
        & > div{
          display: inline-block;
          height: 40px;
          line-height: 40px;
          padding: 0 14px;
          color: #ffffff;
          background: #f1f1f1;
          border-radius: 8px;
          margin-right: 12px;
          cursor: pointer;
        }
        .default-btn{
          color: #787878;
          &:hover{
            color: #509EE3;
          }
        }
        .danger-btn{
          background: #EF8C8C;
          &:hover{
            background: rgb(224, 135, 135);
          }
          &:active{
            background: #EF8C8C;
          }
        }
        .confirm-btn{
          color: #ffffff;
          background: #509EE3;
          &:hover{
            background: #6FB0EB;
          }
        }
      }
      .close-btn{
        position: absolute;
        top: 16px;
        right: 16px;
        width: 30px;
        height: 30px;
        line-height: 30px;
        text-align: center;
        font-size: 18px;
        cursor: pointer;
        &:hover{
          font-weight: 600;
        }
      }
    }
  }
</style>
/*        <!--.tableTitle {-->
<!--  position: relative;-->
<!--  margin: 0 auto;-->
<!--  width: 600px;-->
<!--  height: 1px;-->
<!--  background-color: #d4d4d4;-->
<!--  text-align: center;-->
<!--  font-size: 16px;-->
<!--  color: rgba(101, 101, 101, 1);-->
<!--}-->
<!--.midText {-->
<!--  position: absolute;-->
<!--  left: 50%;-->
<!--  background-color: #ffffff;-->
<!--  padding: 0 15px;-->
<!--  transform: translateX(-50%) translateY(-50%);-->
<!--}-->
<!--.content-title {-->
<!--  position: fixed;-->
<!--  left: 10%;-->
<!--  padding-top:5px;-->
<!--  word-wrap:break-word;-->
<!--}*/-->
