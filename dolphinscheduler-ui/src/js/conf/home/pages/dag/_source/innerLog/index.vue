<template>
  <m-inner-log :taskId="taskId"></m-inner-log>
</template>

<script>
    import innerLog from './innerLog'
    export default {
        name: "inner-log-index",
        data () {
          return {
            taskId: this.$route.params.id,
          }
        },
        // methods:{
        //   _getLog (id) {
        //     console.log("进入到_refreshLog")
        //     let self = this
        //     let instance = this.$modal.dialog({
        //       closable: false,
        //       showMask: true,
        //       escClose: true,
        //       className: 'v-modal-custom',
        //       transitionName: 'opacityp',
        //       render (h) {
        //         return h(mLog, {
        //           on: {
        //             ok () {
        //             },
        //             close () {
        //               instance.remove()
        //             }
        //           },
        //           props: {
        //             self: self,
        //             source: 'list',
        //             logId: id
        //           }
        //         })
        //       }
        //     })
        //   },
        // },
        created () {
          console.log("进入 index ...")
        },
        components: {
          'm-inner-log':innerLog
        }
    }
</script>

<style scoped>

</style>
