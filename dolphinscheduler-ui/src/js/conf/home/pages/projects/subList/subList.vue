<!--<template>-->
<!--  <div class="project-sub-list">-->
<!--    <el-menu :default-active="/project"-->
<!--             background-color="#333744"-->
<!--             active-text-color="#fff000"-->
<!--             unique-opened-->
<!--             :collapse="isCollapse"-->
<!--             :collapse-transition="false"-->
<!--             router>-->
<!--      <el-menu-item index="/project">项目</el-menu-item>-->
<!--      <el-menu-item index="/projects/global-search">全局搜索</el-menu-item>-->
<!--    </el-menu>-->
<!--  </div>-->
<!--</template>-->

<template>
  <div class="project-sub-list">
    <el-container class="el-container">
      <el-aside class="el-aside" width="200px">
        <div class="menu-list" style="height: 100%">
          <!--        <el-menu router>-->
          <el-menu class="menu-index" :default-active="$router.path"
                   background-color="#545c64"
                   text-color="#fff"
                   active-text-color="#ffd04b"
                   unique-opened
                   :collapse="isCollapse"
                   :collapse-transition="false"
                   router>
            <!--          <TreeMenu :tree-data="treeData"/>-->
            <el-menu-item index="/projects/list">项目</el-menu-item>
            <el-menu-item index="/projects/global-search">全局搜索</el-menu-item>
          </el-menu>
        </div>
      </el-aside>
      <el-container>
        <el-main>
          <router-view></router-view>
        </el-main>
      </el-container>
    </el-container>
  </div>
</template>

<script>
  // import treeData from "../mock/index.json";
  // import TreeMenu from "../components/TreeMenu";
  export default {
    name: 'subList',
    // components: {
    //   TreeMenu
    // },
    data() {
      return {
        // treeData
      }
    }
  }
</script>
<style scoped>
  /*.el-container {*/
  /*  height: 100%;*/
  /*}*/
  /*.el-aside {*/
  /*  display: block;*/
  /*  position: absolute;*/
  /*  left: 0;*/
  /*  top: 70px;*/
  /*  bottom: 0;*/
  /*  overflow-y: scroll;*/
  /*}*/
</style>

