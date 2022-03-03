<template>
  <div>
    <x-button
      type="info"
      shape="circle"
      size="xsmall"
      data-toggle="tooltip"
      :title="$t('View log')"
      icon="ans-icon-log"
      @click="_getLog(taskId)">
    </x-button>
  </div>
</template>

<script>
    import mLog from '@/conf/home/pages/dag/_source/formModel/interfaceLog'

    export default {
        name: "innerLog",
        props: {
          taskId: {
            type: Number,
            default: 0
          }
        },
        methods:{
          _getLog (id) {
            console.log("进入到_refreshLog")
            let self = this
            let instance = this.$modal.dialog({
              closable: false,
              showMask: true,
              escClose: true,
              className: 'v-modal-custom',
              transitionName: 'opacityp',
              render (h) {
                return h(mLog, {
                  on: {
                    ok () {
                    },
                    close () {
                      instance.remove()
                    }
                  },
                  props: {
                    self: self,
                    source: 'list',
                    logId: id
                  }
                })
              }
            })
          },
        },
        created () {
          console.log("starting to get log ...")
          this._getLog(this.taskId)
        }
    }
</script>

<style scoped>

</style>
