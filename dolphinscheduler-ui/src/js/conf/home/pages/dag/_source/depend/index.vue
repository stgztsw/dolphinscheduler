<template>
  <div>
    <dialog-bar
      v-model="sendVal"
      :id="id"
      type="danger"
      title="依赖图"
      :workType="workType"
      :relation="relation"
      v-on:cancel="clickCancel()"
      @danger="clickDanger()"
      @confirm="clickConfirm()"
      dangerText="Delete">
    </dialog-bar>
    <el-dialog
      title="提示"
      :visible.sync="dialogVisible"
      width="30%"
      :before-close="handleClose"
      custom-class="dialog-mask">
      <span>这是一段信息</span>
      <span slot="footer" class="dialog-footer">
            <el-button @click="dialogVisible = false">取 消</el-button>
            <el-button type="primary" @click="dialogVisible = false">确 定</el-button>
          </span>
    </el-dialog>
  </div>
</template>
<script>
  // import mSecondaryMenu from '@/module/components/secondaryMenu/secondaryMenu'
  // import mInstanceDetails from '@/conf/home/pages/dag/instanceDetails.vue'
  // import mDependentsView from '@/conf/home/pages/dag/_source/depend/dependentsView.vue'
  // import mDependView from '@/conf/home/pages/dag/_source/depend/depdendView.vue'
  // import maaa from '@/conf/home/pages/dag/aaa.vue'
  import dialogBar from "@/conf/home/pages/dag/_source/depend/dialog/dependDialog";
  // import Affirm from "../jumpAffirm";
  // import _ from "lodash";
  export default {
    name: 'instance-depend-index',
    data () {
      return {
        id: this.$route.params.id,
        relation: this.$route.params.relation,
        workType: this.$route.param.workType,
        // 是否展示依赖窗口
        sendVal: true,
      }
    },
    methods: {
      openMask(){
        this.sendVal = true;
      },
      clickCancel(){
        console.log('点击了取消');
      },
      clickDanger(){
        console.log('这里是danger回调')
      },
      clickConfirm(){
        console.log('点击了confirm');
      },

    },
    components: {
      'dialog-bar': dialogBar
    },

    created() {
      this.id = this.$route.params.id,
      this.relation = this.$route.params.relation,
      this.workType = this.$route.param.workType,
      this.sendVal=true
      console.log(this.$children)
      console.log('instance-depend-index')
    }
  }
</script>
