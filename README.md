# 大数据实习项目

## 一、题目

网易云音乐网站数据分析

## 二、研究主要内容

### 研究内容：  
$$
用户分析
  \begin{cases}
  活跃用户emo指数地图分布\\
  活跃用户emo指数性别分布\\
  活跃用户emo指数年龄分布\\
  地区emo指数前十城市\\
  爬取信息统计展示\\
  各地区评论词云图
  \end{cases}
$$

$$
歌单分析
  \begin{cases}
  歌单评论词云\\
  歌单基础信息展示\\
  歌单歌曲风格分布\\
  歌单歌曲emo指数分布
  \end{cases}
$$

$$
歌曲分析
  \begin{cases}
  听歌用户emo指数地图分布\\
  听歌用户emo指数性别分布\\
  听歌用户emo指数年龄分布\\
  歌曲评论词云图\\
  相似歌曲推荐*
  \end{cases}
$$

$$
歌手分析
\begin{cases}
创作歌曲歌词词云\\
歌手基本信息展示\\
综合评价前三歌曲\\
创作歌曲风格分布\\
创作歌曲emo分布\\
歌手热歌展示
\end{cases}
$$

### 研究方法：

- 开发爬虫程序采集网易云音乐数据
- 使用多进程技术并行化爬虫程序
- 使用hadoop存储网络数据
- 使用spark分析网络数据

### 研究目标：

通过对网易云音乐网站数据分析，可以洞察用户需求、优化用户体验、提升音乐推荐效果，同时也为音乐产业链的各个环节提供决策支持，推动音乐行业的发展。

## 三、主要技术指标

1. 系统需基于B/S结构
2. 采用传统的三层结构方式进行解耦开发
3. 严格按照软件工程的要求实施课题的调研，分析，设计，测试，部署和各阶段管理
4. 数据分析需采用hadoop或者spark