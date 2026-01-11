tools1 = [
    # MEDIA意图
    {
        "type": "function",
        "function": {
            "name": "Open_Play_History",
            "description": "打开或展示或看看指定媒体源的播放历史记录。媒体源可为空",
            "parameters": {
                "type": "object",
                "properties": {
                    "Media_Source": {
                        "type": "string",
                        "description": "若无明确的指定媒体源，则设为空。若有明确的指定媒体源，将其提取并映射为书面表达。比如歌曲，歌单，专辑，在线电台，在线广播等。"
                                       "相声、小品、电视剧请映射为在线电台"
                    }
                },
                "required": ["Media_Source"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Play_History",
            "description": "关闭媒体源播放历史，媒体源可为空",
            "parameters": {
                "type": "object",
                "properties": {
                    "Media_Source": {
                        "type": "string",
                        "description": "若无明确的指定媒体源，则设为空。若有明确的指定媒体源，将其提取并映射为书面表达，比如歌曲，歌单，专辑，在线电台，在线广播等。"
                                       "相声、小品、电视剧请映射为在线电台"
                    }
                },
                "required": ["Media_Source"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Play_List",
            "description": "按照指定媒体源打开对应的播放列表。媒体源可为空。"
                           "或想看有哪些歌是要播放的",
            "parameters": {
                "type": "object",
                "properties": {
                    "Media_Source": {
                        "type": "string",
                        "description": "若无明确的指定媒体源，则设为空。若有明确的指定媒体源，将其提取并映射为书面表达，比如歌曲，歌单，专辑，在线电台，在线广播等。"
                                       "相声、小品、电视剧请映射为在线电台"
                    }
                },
                "required": ["Media_Source"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Play_List",
            "description": "按照指定媒体源关闭对应的播放列表。媒体源可为空",
            "parameters": {
                "type": "object",
                "properties": {
                    "Media_Source": {
                        "type": "string",
                        "description": "若无明确的指定媒体源，则设为空。若有明确的指定媒体源，将其提取并映射为书面表达，比如歌曲，歌单，专辑，在线电台，在线广播等。"
                                       "相声、小品、电视剧请映射为在线电台"
                    }
                },
                "required": ["Media_Source"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Player",
            "description": "打开媒体源或按照指定媒体源打开对应的播放器（主界面），比如打开音乐，打开电台。媒体源可为空。"
                           "或用户询问有没有能放音乐、放歌、放电台、放广播的软件（应用/播放器/主界面）"
                           "如果指定为咪咕音乐、凤凰FM、沐耳FM，则一定不属于此函数",
            "parameters": {
                "type": "object",
                "properties": {
                    "Media_Source": {
                        "type": "string",
                        "description": "若无明确的指定媒体源，则设为空。若有明确的指定媒体源，将其提取并映射为书面表达，比如歌曲，歌单，专辑，在线电台，在线广播，广播，收音机等"
                                       "相声、小品、电视剧请映射为在线电台"
                    }
                },
                "required": ["Media_Source"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Player",
            "description": "按照指定媒体源关闭对应的播放器或关闭对应媒体源，比如关闭音乐，关闭音乐播放器。媒体源可为空。"
                           "如果指定为咪咕音乐、凤凰FM、沐耳FM，则一定不属于此函数",
            "parameters": {
                "type": "object",
                "properties": {
                    "Media_Source": {
                        "type": "string",
                        "description": "若无明确的指定媒体源，则设为空。若有明确的指定媒体源，将其提取并映射为书面表达，比如歌曲，歌单，专辑，在线电台，在线广播等。"
                                       "相声、小品、电视剧请映射为在线电台"
                    }
                },
                "required": ["Media_Source"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Play_Detail",
            "description": "按照指定媒体源打开对应的播放详细信息。媒体源可为空",
            "parameters": {
                "type": "object",
                "properties": {
                    "Media_Source": {
                        "type": "string",
                        "description": "若无明确的指定媒体源，则设为空。若有明确的指定媒体源，将其提取并映射为书面表达，比如歌曲，歌单，专辑，在线电台，在线广播等。"
                                       "相声、小品、电视剧请映射为在线电台"
                    }
                },
                "required": ["Media_Source"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Play_Detail",
            "description": "按照指定媒体源关闭对应的播放详情。媒体源可为空",
            "parameters": {
                "type": "object",
                "properties": {
                    "Media_Source": {
                        "type": "string",
                        "description": "若无明确的指定媒体源，则设为空。若有明确的指定媒体源，将其提取并映射为书面表达，比如歌曲，歌单，专辑，在线电台，在线广播等。"
                                       "相声、小品、电视剧请映射为在线电台"
                    }
                },
                "required": ["Media_Source"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Media_Collection",
            "description": "按照指定媒体源打开对应的收藏夹（关注列表）。媒体源可为空",
            "parameters": {
                "type": "object",
                "properties": {
                    "Media_Source": {
                        "type": "string",
                        "description": "若无明确的指定媒体源，则设为空。若有明确的指定媒体源，将其提取并映射为书面表达，比如歌曲，歌单，专辑，在线电台，在线广播等。"
                                       "相声、小品、电视剧请映射为在线电台"
                    }
                },
                "required": ["Media_Source"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Media_Collection",
            "description": "按照指定媒体源关闭对应的收藏夹（关注列表）。媒体源可为空。"
                           "只有用户明确表达要听收藏夹（关注列表）里的内容，才命中此函数",
            "parameters": {
                "type": "object",
                "properties": {
                    "Media_Source": {
                        "type": "string",
                        "description": "若无明确的指定媒体源，则设为空。若有明确的指定媒体源，将其提取并映射为书面表达，比如歌曲，歌单，专辑，在线电台，在线广播等。"
                                       "相声、小品、电视剧请映射为在线电台"
                    }
                },
                "required": ["Media_Source"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Play_Media_Collection",
            "description": "按照媒体源播放对应的收藏夹里的收藏（关注）内容。"
                           "只有用户明确表达要听收藏夹（关注）里的内容，才命中此函数。歌单不算收藏夹",
            "parameters": {
                "type": "object",
                "properties": {
                    "Collect_Source": {
                        "type": "string",
                        "description": "媒体收藏源包括但不限于歌曲，歌单，在线电台，在线广播，本地电台，专辑，自建歌单。若无则为空"
                    }
                },
                "required": ["Collect_Source"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Play_History",
            "description": "按照指定媒体源播放对应的播放记录（历史），而非显示或看看播放记录（历史）。媒体源可为空",
            "parameters": {
                "type": "object",
                "properties": {
                    "Media_Source": {
                        "type": "string",
                        "description": "若无明确的指定媒体源，则设为空。若有明确的指定媒体源，将其提取并映射为书面表达，比如歌曲，歌单，专辑，在线电台，在线广播等。"
                                       "相声、小品、电视剧请映射为在线电台"
                    }
                },
                "required": ["Media_Source"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Play_Similar_Music",
            "description": "播放相似歌曲",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "View_Play_Content",
            "description": "用户想要查看当前正在播放的内容",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "View_Play_Radio",
            "description": "用户想要查询当前播放的电台信息，比如关于什么话题的、什么类型的。"
                           "仅当用户提到了刚才的电台是啥节目等，则将Just提取为1。否则，不提取Just",
            "parameters": {
                "type": "object",
                "properties": {
                    "Just":{
                        "type": "string",
                        "description": "如果用户询问刚才的电台或上一首电台的信息，请提取为1"
                    }
                }
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "View_Play_Music",
            "description": "用户想要查询当前播放的歌曲信息。"
                           "仅当用户提到了刚才的是啥歌等，则将Just提取为1。否则，不提取Just",
            "parameters": {
                "type": "object",
                "properties": {
                    "Just": {
                        "type": "string",
                        "description": "如果用户询问刚才的歌曲或上一首歌曲的信息，请提取为1"
                    }
                }
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "View_Play_Music_Singer",
            "description": "用户想要查询当前播放的歌曲的歌手信息。"
                           "如果用户说了具体歌名，不命中此函数，此函数不需要提取任何参数",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "View_Play_Frequency",
            "description": "用户想要查询当前播放的广播频道。"
                           "仅当用户提到了刚才放的是什么频道等，则将Just提取为1。否则，不提取Just",
            "parameters": {
                "type": "object",
                "properties": {
                    "Just": {
                        "type": "string",
                        "description": "如果用户询问刚才的频道或上一个广播的频道，请提取为1"
                    }
                }
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "View_Play_News",
            "description": "用户想要查询当前播放的新闻相关信息。"
                           "仅当用户提到了刚才放的是什么新闻等，则将Just提取为1。否则，不提取Just",
            "parameters": {
                "type": "object",
                "properties": {
                    "Just": {
                        "type": "string",
                        "description": "如果用户询问刚才的新闻或上一首新闻的信息，请提取为1"
                    }
                }
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "View_Play_Album",
            "description": "用户想要查询当前播放的专辑相关信息。"
                           "仅当用户提到了刚才放的是什么专辑等，则将Just提取为1。否则，不提取Just",
            "parameters": {
                "type": "object",
                "properties": {
                    "Just": {
                        "type": "string",
                        "description": "如果用户询问刚才的专辑或上一个专辑的信息，请提取为1"
                    }
                }
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Get_exact_time_For_Play",
            "description": "按照精确到分和秒的时间设置播放的起始时间",
            "parameters": {
                "type": "object",
                "properties": {
                    "time": {
                        "type": "string",
                        "description": "精确到分秒的时间。比如10分6秒，四十五秒，11分。请规范为分:秒的格式，比如22:23、00:15"
                    }
                },
                "required": ["time"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_lyrics",
            "description": "用户想看一首歌曲的歌词",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_lyrics",
            "description": "关闭一首歌曲的歌词",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Play_repeatly",
            "description": "用户想要单曲循环播放，或循环播放专辑、音乐、电台、相声、小品、新闻等",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Continue_Play",
            "description": "继续播放或接着播放指定媒体源内容。媒体源可为空",
            "parameters": {
                "type": "object",
                "properties": {
                    "Media_Source": {
                        "type": "string",
                        "description": "若无明确的指定媒体源，则设为空。若有明确的指定媒体源，将其提取并映射为书面表达，比如歌曲，歌单，专辑，在线电台，在线广播等。"
                                       "相声、小品、电视剧请映射为在线电台"
                    }
                },
                "required": ["Media_Source"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Quick_Go",
            "description": "让播放的指定媒体源内容快进指定时长。媒体源可为空，指定时长可为空",
            "parameters": {
                "type": "object",
                "properties": {
                    "Media_Source": {
                        "type": "string",
                        "description": "若无明确的指定媒体源，则设为空。若有明确的指定媒体源，将其提取并映射为书面表达，比如歌曲，歌单，专辑，在线电台，在线广播等。"
                                       "相声、小品、电视剧请映射为在线电台"
                    },
                    "Time": {
                        "type": "string",
                        "description": "时长，请规范为xx:xx的格式。比如00:15、03:00等。若无则为空"
                    }
                },
                "required": ["Media_Source", "Time"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Quick_Back",
            "description": "让播放的指定媒体源内容快速回退指定时长。媒体源可为空，指定时长可为空",
            "parameters": {
                "type": "object",
                "properties": {
                    "Media_Source": {
                        "type": "string",
                        "description": "若无明确的指定媒体源，则设为空。若有明确的指定媒体源，将其提取并映射为书面表达，比如歌曲，歌单，专辑，在线电台，在线广播等。"
                                       "相声、小品、电视剧请映射为在线电台"
                    },
                    "Time": {
                        "type": "string",
                        "description": "时长，请规范为xx:xx的格式。比如00:15、03:00等。若无则为空"
                    }
                },
                "required": ["Media_Source", "Time"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Change_Play_Mode",
            "description": "用户想要改变或切换播放模式",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Cancel_Play_Mode",
            "description": "用户想要退出当前播放模式",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Media_Collect",
            "description": "收藏（关注）内容或把某内容放入指定媒体源的收藏夹。媒体源可为空",
            "parameters": {
                "type": "object",
                "properties": {
                    "Media_Source": {
                        "type": "string",
                        "description": "若无明确的指定媒体源，则设为空。若有明确的指定媒体源，将其提取并映射为书面表达，比如歌曲，歌单，专辑，在线电台，在线广播等。"
                                       "相声、小品、电视剧请映射为在线电台"
                    }
                },
                "required": ["Media_Source"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Cancel_Media_Collect",
            "description": "取消收藏内容或把某内容移出对应的指定媒体源收藏夹。媒体源可为空",
            "parameters": {
                "type": "object",
                "properties": {
                    "Media_Source": {
                        "type": "string",
                        "description": "若无明确的指定媒体源，则设为空。若有明确的指定媒体源，将其提取并映射为书面表达，比如歌曲，歌单，专辑，在线电台，在线广播等。"
                                       "相声、小品、电视剧请映射为在线电台"
                    }
                },
                "required": ["Media_Source"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Media_Last",
            "description": "用户想要重新播放上一首或前一个内容,包括播放上一首音乐、上一个相声小品、上一个新闻等等",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Media_Next",
            "description": "用户想要播放下一首或后一个内容,包括播放下一首音乐、下一个相声小品、下一个新闻等等。"
                           "或用户想跳过这个、不想听这首、不需要这个、这个不好听换一个等。"
                           "如果用户只说不想听了，不命中此函数"
                           "如果用户说声音不好听，不命中此函数",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Media_Order_Play",
            "description": "用户想要按顺序播放媒体源里的内容，比如歌单、专辑、新闻、电台、相声、小品、栏目、音乐等",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Media_Random_Play",
            "description": "用户想要随机播放媒体源内容，比如歌单、专辑、新闻、电台、相声、小品、栏目、音乐等。"
                           "或用户不想顺序播放或循环播放媒体源内容",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Download_Current_Content",
            "description": "用户想要下载当前播放内容",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Replay",
            "description": "用户想要重新播放一次当前内容，或想再听一下刚才放的内容。"
                           "播放这首歌不命中此函数",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Media_Pause",
            "description": "暂停播放或停止播放当前对应的媒体源内容。媒体源可为空。"
                           "或让媒体源闭嘴。或用户说太难听了、不想听了",
            "parameters": {
                "type": "object",
                "properties": {
                    "Media_Source": {
                        "type": "string",
                        "description": "若无明确的指定媒体源，则设为空。若有明确的指定媒体源，将其提取并映射为书面表达，比如歌曲，歌单，专辑，在线电台，在线广播等。"
                                       "相声、小品、电视剧请映射为在线电台"
                    }
                },
                "required": ["Media_Source"]
            },
        }
    },



    # AUTO意图
    {
        "type": "function",
        "function": {
            "name": "Close_AutoHold",
            "description": "用户想要关闭汽车的自动驻车功能，即想要自己控制刹车",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Engine_AutoStop",
            "description": "用户想要关闭汽车的自动启停功能，即短暂停车时不再熄火",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_AutoHold",
            "description": "用户想要打开汽车的自动驻车功能，即想要停车时不再控制刹车",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Engine_AutoStop",
            "description": "用户想要打开汽车的自动启停功能，即短暂停车等待时自动熄火",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Air_Cleaner",
            "description": "用户想要关闭空气净化器或关闭空气除味功能",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Air_Cleaner",
            "description": "用户想要打开空气净化器或打开空气除味功能",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Wind_Direction",
            "description": "将空调向指定方向吹或让空调向指定方向吹，或打开空调指定方向吹风。需要对指定方向进行映射",
            "parameters": {
                "type": "object",
                "properties": {
                    "Direction": {
                        "type": "string",
                        "description": "仅包括吹脸、吹脚、吹脸吹脚、吹窗吹脚、吹窗"
                    }
                },
                "required": ["Direction"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Cancel_Wind_Direction",
            "description": "取消将空调向指定方向吹或让空调不再向指定方向吹，需要对指定方向进行映射",
            "parameters": {
                "type": "object",
                "properties": {
                    "Direction": {
                        "type": "string",
                        "description": "仅包括吹脸、吹脚、吹脸吹脚、吹窗吹脚、吹窗"
                    }
                },
                "required": ["Direction"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Air_Condition",
            "description": "用户仅想关闭空调或仅关闭指定座位的空调。需要对指定座位进行映射，若未指定座位，则座位为空。若未指定空调，不命中此函数",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "若无指定座位，则设为空；若有指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    }
                },
                "required": ["Position"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Air_Condition_Auto_Mode",
            "description": "关闭空调的自动模式或关闭指定座位的空调自动模式，或者用户想要手动控制空调。需要对指定座位进行映射。指定座位可为空。若未指定自动空调，不命中此函数",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无指定座位，则设为空；若有指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    }
                },
                "required": ["Position"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_AC",
            "description": "用户想要关闭空调压缩器或AC",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Cooling",
            "description": "用户想要关闭空调制冷模式",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Heating",
            "description": "用户想要关闭空调制热模式",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Cooling_Instant",
            "description": "用户想要关闭一键降温",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Air_Condition_Sync",
            "description": "用户想要关闭空调温度同步模式或者想要独立设置每个座位的空调温度。指定座位可为空",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无指定座位，则设为空；若有指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    }
                },
                "required": ["Position"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Wind_Auto_Mode",
            "description": "用户想要关闭自动风向或者想要自己设置空调风向",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Air_Condition_Defog",
            "description": "关闭空调除雾除霜功能或关闭指定座位的空调除雾除霜功能。需要对指定座位进行映射。指定座位可为空",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无指定座位，则设为空；若有指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    }
                },
                "required": ["Position"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_External_Circulation",
            "description": "用户想要关闭空调外循环",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Internal_Circulation",
            "description": "用户想要关闭空调内循环",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Dec_Air_Condition_Temperature",
            "description": "按照汽车内座位位置把空调打开并将空调温度调低某值，如果某值是一个区间，从区间随便取一个数即可。Number和Ratio只能提取一个。指定座位可为空。"
                           "若仅调低空调温度也命中此函数。"
                           "若用户说小一点、够热了等，将NUMBER提取为1，不提取RATIO",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无指定座位，则设为空；若有指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    },
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                },
                "required": ["Position", "Number", "Ratio"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Dec_Air_Condition_Wind",
            "description": "按照汽车内座位位置把空调打开并将空调风力调低某值，而非降到。如果某值是一个区间，从区间随便取一个数即可，需要进行映射。Number和Ratio只能提取一个。指定座位可为空。"
                           "若仅调低空调风力，也命中此函数。"
                           "若用户说小一点、够大了等，将NUMBER提取为1，不提取RATIO",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无指定座位，则设为空；若有指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    },
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                },
                "required": ["Position", "Number", "Ratio"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Inc_Air_Condition_Temperature",
            "description": "按照汽车内座位位置把空调打开并将温度调高某值，如果某值是一个区间，从区间随便取一个数即可。Number和Ratio只能提取一个。指定座位可为空。"
                           "若仅调高空调温度也命中此函数。"
                           "若用户说大一点、够冷了等，将NUMBER提取为1，不提取RATIO",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无指定座位，则设为空；若有指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    },
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                },
                "required": ["Position", "Number", "Ratio"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Heating_Instant",
            "description": "用户想要开启空调快速升温功能或用户现在非常冷",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Inc_Air_Condition_Wind",
            "description": "按照汽车内座位位置把空调打开并将空调风力调高某值，如果某值是一个区间，从区间随便取一个数即可，需要进行映射。Number和Ratio只能提取一个。指定座位可为空。"
                           "若仅调高空调风力，也命中此函数。"
                           "若用户说大一点、风不够大等，将NUMBER提取为1，不提取RATIO",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无指定座位，则设为空；若有指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    },
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                },
                "required": ["Position", "Number", "Ratio"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Air_Condition",
            "description": "用户仅要打开空调或仅打开指定座位的空调。需要对指定座位进行映射，若未指定，则位置为空。",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无指定座位，则设为空；若有指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    }
                },
                "required": ["Position"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Air_Condition_Auto_Mode",
            "description": "打开空调的自动模式或打开指定座位的空调自动模式，或者用户想要自动空调。需要对指定座位进行映射。指定座位可为空",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无指定座位，则设为空；若有指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    }
                },
                "required": ["Position"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_AC",
            "description": "用户想要打开空调压缩器或AC",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Cooling",
            "description": "用户想要打开空调制冷模式",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Heating",
            "description": "用户想要打开空调制热模式",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Cooling_Instant",
            "description": "用户想要打开一键降温功能或者用户现在非常热",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Air_Condition_Sync",
            "description": "用户想要打开空调温度同步模式或者想要统一设置指定座位的空调温度。指定座位可为空",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无指定座位，则设为空；若有指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    }
                },
                "required": ["Position"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Wind_Auto_Mode",
            "description": "用户想要打开自动风向或者想要空调自动控制风向",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Air_Condition_Defog",
            "description": "打开空调除雾除霜功能或打开指定座位的空调除雾除霜功能。需要对指定座位进行映射。指定座位可为空",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无指定座位，则设为空；若有指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    }
                },
                "required": ["Position"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_External_Circulation",
            "description": "用户想要打开空调外循环",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Internal_Circulation",
            "description": "用户想要打开空调内循环",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Air_Condition_Temperature",
            "description": "按照汽车内座位位置打开空调并设置空调温度或打开空调并把空调温度调到某值或极端程度，如果某值是一个区间，从区间随便取一个数即可。"
                           "Number,Ratio和Extreme只能提取一个。指定座位可为空。"
                           "若仅设置空调温度也命中此函数。",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无指定座位，则设为空；若有指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    },
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                    "Extreme": {
                        "type": "string",
                        "description": "提取出包含“最”字的词组，并根据词义映射为最高或者最低的其中一个。不包含“最”字则为空"
                    }
                },
                "required": ["Position", "Number", "Ratio", "Extreme"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Air_Condition_Wind",
            "description": "按照汽车内座位位置设置空调风力或把空调风力调（降/升）到某值（档位）或极端程度，如果某值（某档位）是一个区间，从区间随便取一个数即可。"
                           "Number,Ratio和Extreme只能提取一个。指定座位可为空",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无指定座位，则设为空；若有指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    },
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                    "Extreme": {
                        "type": "string",
                        "description": "提取出包含“最”字的词组，并根据词义映射为最高或者最低的其中一个。不包含“最”字则为空"
                    }
                },
                "required": ["Position", "Number", "Ratio", "Extreme"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Surround_View",
            "description": "关闭环绕摄像（倒车影像/倒车雷达）或汽车360度全景摄像",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Surround_View",
            "description": "打开环绕摄像（倒车影像/倒车雷达）或汽车360度全景摄像",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Surround_View",
            "description": "打开用户指定摄像头",
            "parameters": {
                "type": "object",
                "properties": {
                    "Camera": {
                        "type": "string",
                        "description": "包括但不限于前视摄像头、车内摄像头等"
                    }
                }
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Wireless_Charge",
            "description": "关闭车内的无线充电功能",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Wireless_Charge",
            "description": "打开车内的无线充电功能",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_DashCam",
            "description": "关闭车里的行车记录仪，或关闭行车记录仪的界面（APP）",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_DashCam",
            "description": "打开车里的行车记录仪，或打开行车记录仪的界面（APP）",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Record_Audio",
            "description": "用户要求开始录音，即让行车记录仪记录车内的所有声音",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Record_Video",
            "description": "用户要求开始录像，即让行车记录仪持续记录画面",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Stop_Audio",
            "description": "用户要求停止录音，即让行车记录仪结束记录车内声音",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Stop_Video",
            "description": "用户要求停止录像，即让行车记录仪不再持续记录视频画面",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Take_Photo",
            "description": "用户要求拍照，即让行车记录仪保存下当前画面",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Dormer",
            "description": "关闭天窗，即关闭位于车顶的那扇窗户",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Dormer",
            "description": "打开天窗，即打开位于车顶的那扇窗户",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Driving_Mode",
            "description": "询问驾驶模式或将汽车驾驶模式设为指定的驾驶模式，需要进行映射",
            "parameters": {
                "type": "object",
                "properties": {
                    "Mode": {
                        "type": "string",
                        "description": "仅包括经济模式、舒适模式、运动模式和正常模式。若无则为空"
                    }
                },
                "required": ["Mode"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Env_Light",
            "description": "关闭氛围灯或车内灯",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Env_Light_Auto_Mode",
            "description": "关闭氛围灯或车内灯的自动模式或用户想要手动控制氛围灯",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Env_Light_Auto_Mode",
            "description": "打开氛围灯或车内灯的自动模式或用户想要能够自动控制的氛围灯",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Env_Light",
            "description": "打开氛围灯或车内灯，或用户说车里太黑了",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Dec_Env_Light_Brightness",
            "description": "按照数字或百分数把氛围灯打开并将亮度调低某值，如果某值是一个区间，从区间随便取一个数即可。Number和Ratio只能提取一个。"
                           "若仅将氛围灯亮度调低，也命中此函数。"
                           "若用户说小一点、够亮了等，将NUMBER提取为1，不提取RATIO",
            "parameters": {
                "type": "object",
                "properties": {
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                },
                "required": ["Number", "Ratio"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Inc_Env_Light_Brightness",
            "description": "按照数字把氛围灯打开并将亮度调高某值，如果某值是一个区间，从区间随便取一个数即可。Number和Ratio只能提取一个。"
                           "若仅调高氛围灯亮度，也命中此函数。"
                           "若用户说亮一点、不够亮了等，将NUMBER提取为1，不提取RATIO",
            "parameters": {
                "type": "object",
                "properties": {
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                },
                "required": ["Number", "Ratio"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Env_Light_Brightness",
            "description": "按照数字把氛围灯打开并将亮度调节到某值（档位）或极端程度，如果某值（某档位）是一个区间，从区间随便取一个数即可。"
                           "Number,Ratio和Extreme只能提取一个。"
                           "若仅调节氛围灯亮度也命中此函数",
            "parameters": {
                "type": "object",
                "properties": {
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                    "Extreme": {
                        "type": "string",
                        "description": "提取出包含“最”字的词组，并根据词义映射为最高或者最低的其中一个。不包含“最”字则为空"
                    }
                },
                "required": ["Number", "Ratio", "Extreme"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Env_Light_Color",
            "description": "将氛围灯打开并将氛围灯设置为指定颜色。"
                           "若仅设置氛围灯颜色也命中此函数",
            "parameters": {
                "type": "object",
                "properties": {
                    "Color": {
                        "type": "string",
                        "description": "形容颜色的词语"
                    }
                },
                "required": ["Color"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Env_Light_Theme",
            "description": "按照指定的氛围灯主题来打开并设置氛围灯主题。需要进行映射",
            "parameters": {
                "type": "object",
                "properties": {
                    "Theme": {
                        "type": "string",
                        "description": "仅包括沉醉、活力、渴望、无限、永恒。"
                    }
                },
                "required": ["Theme"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_ADAPTIVE_HEAPLAMP",
            "description": "关闭自动大灯功能或者用户不想让车外灯光自动亮起",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_ADAPTIVE_HEAPLAMP",
            "description": "打开自动大灯功能或者用户想当外部光线较弱时车外灯光自动亮起",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Fog_Light",
            "description": "关闭雾灯，即关闭在大雾环境中能提高可见度的灯光，同时关闭前雾灯和后雾灯",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Fog_Light",
            "description": "打开雾灯，即打开在大雾环境中能提高可见度的灯光，同时打开前雾灯和后雾灯",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Back_Fog_Light",
            "description": "只关闭后雾灯，即关闭位于车辆后部的雾灯",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Back_Fog_Light",
            "description": "只打开后雾灯，即打开位于车辆后部的雾灯",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Front_Fog_Light",
            "description": "只关闭前雾灯，即关闭位于车辆前部的雾灯",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Front_Fog_Light",
            "description": "只打开前雾灯，即打开位于车辆前部的雾灯",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Headlamp",
            "description": "关闭车前照明的大灯",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Headlamp",
            "description": "打开车前照明的大灯",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_High_Beam",
            "description": "关闭远光灯，即关闭很黑暗或照明条件不好环境中能提高可见度的灯光",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_High_Beam",
            "description": "打开远光灯，即打开很黑暗或照明条件不好的环境中能提高可见度的灯光",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Low_Beam",
            "description": "关闭近光灯，即关闭夜晚照明条件良好环境中使用的灯光",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Low_Beam",
            "description": "打开近光灯或低束灯，即打开夜晚照明条件良好环境中使用的灯光",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Marker_Light",
            "description": "关闭示宽灯",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Marker_Light",
            "description": "打开示宽灯",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Heated_Seat",
            "description": "关闭或不用开启座椅加热或关闭指定座位的座椅加热，需对指定座位进行映射。指定座位可为空",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无指定座位，则设为空；若有指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾）即前排"
                    }
                },
                "required": ["Position"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Heated_Seat",
            "description": "打开座椅加热或打开指定座位的座椅加热。指定座位可为空。或用户说屁股和背有点冷"
                           "若用户还有调整（调高/调低）座椅温度的意图，则不命中此函数",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无指定座位，则设为空；若有指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    }
                },
                "required": ["Position"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Dec_Seat_Temperature",
            "description": "按照汽车内座位位置把座椅加热打开并将座椅温度调低某值，如果某值是一个区间，从区间随便取一个数即可。Number和Ratio只能提取一个。若无指定座位，则设为空。"
                           "若仅调低座椅温度，也命中此函数。或用户说屁股和背太热"
                           "若用户说小一点、够热了等，将NUMBER提取为1，不提取RATIO",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "若无指定座位，则设为空；若有指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    },
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                },
                "required": ["Position", "Number", "Ratio"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Inc_Seat_Temperature",
            "description": "按照汽车内座位位置把座椅加热打开并将温度调高某值，如果某值是一个区间，从区间随便取一个数即可，需要进行映射。Number和Ratio只能提取一个。指定座位可为空。"
                           "若仅调高座椅温度，也命中此函数。"
                           "若用户说大一点、有点凉等，将NUMBER提取为1，不提取RATIO",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无指定座位，则设为空；若有指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    },
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                },
                "required": ["Position", "Number", "Ratio"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Seat_Temperature",
            "description": "按照汽车内座位位置把打开（启动）座椅加热并将座椅温度调整到指定值或极端程度，如果指定值是一个区间，从区间随便取一个数即可，需要进行映射。"
                           "Number,Ratio和Extreme只能提取一个。指定座位可为空。"
                           "若仅调节座椅温度也命中此函数",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无指定座位，则设为空；若有指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    },
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号或一半等表示，就为百分数。若无则为空"
                    },
                    "Extreme": {
                        "type": "string",
                        "description": "提取出包含“最”字的词组，并根据词义映射为最高或者最低的其中一个。不包含“最”字则为空"
                    }
                },
                "required": ["Position", "Number", "Ratio", "Extreme"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Adjust_HUD_Horizon",
            "description": "将HUD位置向水平方向移动，即左移或右移。需要进行映射",
            "parameters": {
                "type": "object",
                "properties": {
                    "Direction": {
                        "type": "string",
                        "description": "仅包括左、右"
                    }
                },
                "required": ["Direction"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Adjust_HUD_Vert",
            "description": "将HUD位置向垂直方向移动，即上移或下移或调低调高。需要进行映射",
            "parameters": {
                "type": "object",
                "properties": {
                    "Direction": {
                        "type": "string",
                        "description": "仅包括上、下"
                    }
                },
                "required": ["Direction"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_HUD",
            "description": "关闭投影或HUD，即不使用抬头显示器",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_HUD",
            "description": "打开投影或HUD，即用户要使用抬头显示器",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Dec_HUD_Brightness",
            "description": "调低HUD的亮度",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Inc_HUD_Brightness",
            "description": "调高HUD的亮度",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Adjust_Hud_Brightness",
            "description": "调节HUD屏幕的亮度，有1，2，3，4，5个档位,亮一点按照1个档位增加，最多不超过5档，暗一点按照1个档位减少，最低不低于1档",
            "parameters": {
                "type": "object",
                "properties": {
                    "level": {
                        "type": "string",
                        "description": "亮度档位，有1，2，3，4，5个档位",
                    },
                },
                "required": ["level"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Reading_Light",
            "description": "关闭阅读灯",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Reading_Light",
            "description": "打开阅读灯",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Rearview_Mirror",
            "description": "用户需要展开车的后视镜",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Rearview_Mirror",
            "description": "用户需要折叠或收起车的后视镜",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Rearview_Mirror_Heating",
            "description": "用户需要关闭后视镜的加热功能",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Rearview_Mirror_Heating",
            "description": "用户需要打开后视镜的加热功能",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Seat_Massage",
            "description": "关闭座椅按摩功能或关闭指定座位的座椅按摩，需对指定座位进行映射。指定座位可为空",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无明确的指定座位，则设为空；若有明确的指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    }
                },
                "required": ["Position"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Dec_Seat_Massage",
            "description": "按照指定座位把座椅按摩打开并将按摩力度调小某值或某档位，如果某值（某档位）是一个区间，从区间随便取一个数即可，需要进行映射。Number和Ratio只能提取一个。指定座位可为空。"
                           "若仅调低座椅按摩力度，也命中此函数。"
                           "若用户说小一点等，将NUMBER提取为1，不提取RATIO",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无明确的指定座位，则设为空；若有明确的指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    },
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                },
                "required": ["Position", "Number", "Ratio"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Inc_Seat_Massage",
            "description": "按照指定座位把座椅按摩打开并将力度调大某值或某档位，如果某值（某档位）是一个区间，从区间随便取一个数即可，需要进行映射。Number和Ratio只能提取一个。指定座位可为空。"
                           "若仅调大座椅按摩力度也命中此函数。"
                           "若用户说大一点，不够舒服等，将NUMBER提取为1，不提取RATIO",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无明确的指定座位，则设为空；若有明确的指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    },
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                },
                "required": ["Position", "Number", "Ratio"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Seat_Massage",
            "description": "打开座椅按摩功能或打开指定座位的座椅按摩，需对指定座位进行映射。指定座位可为空。"
                           "若用户还有调整（调高/调低）座椅按摩力度的意图，则不命中此函数",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无明确的指定座位，则设为空；若有明确的指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    }
                },
                "required": ["Position"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Seat_Massage",
            "description": "按照指定座位把座椅按摩打开并将力度调节到或调节至指定值或极端程度，如果指定值是一个区间，从区间随便取一个数即可，需要进行映射。"
                           "Number,Ratio和Extreme只能提取一个。指定座位可为空。"
                           "若仅调节按摩力度也命中此函数",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无明确的指定座位，则设为空；若有明确的指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    },
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                    "Extreme": {
                        "type": "string",
                        "description": "提取出包含“最”字的词组，并根据词义映射为最高或者最低的其中一个。不包含“最”字则为空"
                    }
                },
                "required": ["Position", "Number", "Ratio", "Extreme"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Adjust_Seat_Long",
            "description": "将座椅位置前后移动，即前移或者后移。需要进行映射",
            "parameters": {
                "type": "object",
                "properties": {
                    "Direction": {
                        "type": "string",
                        "description": "仅包括前、后"
                    }
                },
                "required": ["Direction"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Adjust_Seat_Vert",
            "description": "将座椅位置上下移动，即上移或者下移或者调高调低。需要进行映射",
            "parameters": {
                "type": "object",
                "properties": {
                    "Direction": {
                        "type": "string",
                        "description": "仅包括上、下"
                    }
                },
                "required": ["Direction"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Seat_Ventilation",
            "description": "关闭座椅通风功能或关闭指定座位的座椅通风功能，需对指定座位进行映射。指定座位可为空",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无明确的指定座位，则设为空；若有明确的指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    }
                },
                "required": ["Position"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Dec_Seat_Ventilation",
            "description": "按照指定座位把座椅通风打开并将档位调小某值或某档位，如果某值（某档位）是一个区间，从区间随便取一个数即可，需要进行映射。Number和Ratio只能提取一个。指定座位可为空。"
                           "若仅调低座椅通风档位，也命中此函数。"
                           "若用户说小一点等，将NUMBER提取为1，不提取RATIO",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无明确的指定座位，则设为空；若有明确的指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    },
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                },
                "required": ["Position", "Number", "Ratio"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Inc_Seat_Ventilation",
            "description": "按照指定座位把座椅通风打开并将档位调大某值或某档位，如果某值（某档位）是一个区间，从区间随便取一个数即可，需要进行映射。Number和Ratio只能提取一个。指定座位可为空。"
                           "若仅调大座椅通风也命中此函数。"
                           "若用户说大一点等，将NUMBER提取为1，不提取RATIO",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无明确的指定座位，则设为空；若有明确的指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    },
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                },
                "required": ["Position", "Number", "Ratio"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Seat_Ventilation",
            "description": "打开座椅通风功能或打开指定座位的座椅通风功能，需对指定座位进行映射。指定座位可为空。"
                           "若用户还有调整（调高/调低）座椅通风大小的意图，则不命中此函数",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无明确的指定座位，则设为空；若有明确的指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    }
                },
                "required": ["Position"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Seat_Ventilation",
            "description": "按照指定座位把座椅通风打开并把档位调节到或调节至指定值或极端程度，如果指定值是一个区间，从区间随便取一个数即可，需要进行映射。"
                           "Number,Ratio和Extreme只能提取一个。指定座位可为空。"
                           "若仅调节座椅通风档位，也命中此函数",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无明确的指定座位，则设为空；若有明确的指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    },
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                    "Extreme": {
                        "type": "string",
                        "description": "提取出包含“最”字的词组，并根据词义映射为最高或者最低的其中一个。不包含“最”字则为空"
                    }
                },
                "required": ["Position", "Number", "Ratio", "Extreme"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Heated_Steer",
            "description": "用户需要关闭对方向盘的加热功能",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Heated_Steer",
            "description": "用户需要打开对方向盘的加热功能",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Dec_Steer_Temperature",
            "description": "按照数字把方向盘温度调低某值，如果某值是一个区间，从区间随便取一个数即可",
            "parameters": {
                "type": "object",
                "properties": {
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                },
                "required": ["Number", "Ratio"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Inc_Steer_Temperature",
            "description": "按照数字把方向盘温度调高",
            "parameters": {
                "type": "object",
                "properties": {
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                },
                "required": ["Number", "Ratio"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Steer_Temperature",
            "description": "把方向盘温度调到指定值或极端程度，如果指定值是一个区间，从区间随便取一个数即可。Number,Ratio和Extreme只能提取一个",
            "parameters": {
                "type": "object",
                "properties": {
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                    "Extreme": {
                        "type": "string",
                        "description": "提取出包含“最”字的词组，并映射为最高或者最低的其中一个。不包含“最”字则为空"
                    }
                },
                "required": ["Number", "Ratio", "Extreme"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Sunshade",
            "description": "关闭遮阳帘或把遮阳帘收起或拉上去或卷起来，或用户觉得开车时遮阳帘很挡视线",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Sunshade",
            "description": "打开遮阳帘或把遮阳帘往下放，或用户觉得开车时太阳很晃眼睛",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Air_Condition_APP",
            "description": "关闭控制空调的应用或APP",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Air_Condition_APP",
            "description": "打开控制空调的应用或APP",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Trunk",
            "description": "关闭汽车的后备箱或尾门",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Trunk",
            "description": "打开汽车的后备箱或尾门，或者用户想要在车上放大件行李",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Window",
            "description": "按照指定位置和百分数来打开车窗并调整车窗开合大小。指定座位可为空。"
                           "若仅调整车窗开合大小，也命中此函数",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若没有指定座位，则设为空；若有指定座位，必须将其映射为以下选项的其中一个：主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "仅包括百分数和分数，并表示为小数",
                    },
                },
                "required": ["Position", "Ratio"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Window",
            "description": "按照车内位置来打开车窗。指定座位可为空。"
                           "若用户还有调整车窗开合大小的意思，不命中此函数",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无指定座位，则设为空；若有指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    },
                },
                "required": ["Position"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Window",
            "description": "按照车内位置来关闭车窗。指定座位可为空",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "指定座位。若无指定座位，则设为空；若有指定座位，将其映射为主驾、副驾、前排、后排、右侧、左侧、右后、左后、主对角、副对角、所有的其中一个。"
                                       "主对角必须同时包括主驾和后排右边的两个座位；副对角必须同时包括副驾和后排左边的两个座位；"
                                       "左侧包括主驾座位和后排左边的两个座位；右侧包括副驾座位和后排右边的两个座位；"
                                       "左后代表后排左边座位，即主驾后面的座位；右后代表后排右边座位，即副驾后面的座位"
                                       "主副驾（主驾和副驾），即前排"
                    },
                },
                "required": ["Position"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Window_Diagonal",
            "description": "关闭通风模式",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Window_Diagonal",
            "description": "打开通风模式或用户想要给车里换气",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Wiper",
            "description": "关闭雨刷器或用户觉得雨刷器有点晃眼睛",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Wiper",
            "description": "打开雨刷器或用户觉得在挡风玻璃上的雨水很挡视线",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Bluetooth",
            "description": "关闭车里的蓝牙",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Bluetooth",
            "description": "打开车里的蓝牙",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Desktop_Style",
            "description": "将桌面设置为指定的桌面样式",
            "parameters": {
                "type": "object",
                "properties": {
                    "Desktop": {
                        "type": "string",
                        "description": "仅包括壁纸桌面和地图桌面。需要进行映射，若未指定则设为空"
                    }
                },
                "required": ["Desktop"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Complains_And_Suggestions",
            "description": "用户需要投诉或反馈问题",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Owner_Service",
            "description": "关闭车主服务或用户管家服务",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Owner_Service",
            "description": "打开车主服务或用户管家服务，即打开为车主提供服务功能的页面",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Help",
            "description": "关闭帮助中心或用车助手",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Help",
            "description": "打开帮助中心或用车助手",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Feedback",
            "description": "关闭反馈中心",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Feedback",
            "description": "打开用户反馈中心",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Download_App",
            "description": "按照用户指定应用来下载应用",
            "parameters": {
                "type": "object",
                "properties": {
                    "app": {
                        "type": "string",
                        "description": "包括但不限于QQ音乐车机版、火山车娱等"
                    }
                },
                "required": ["app"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_APP_Store",
            "description": "打开应用商城，或用户想下载app或应用",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_APP_Store",
            "description": "关闭应用商城",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Message",
            "description": "关闭消息中心或界面",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Message",
            "description": "打开消息中心或界面",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "View_All_Message",
            "description": "用户想要查看所有消息",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "View_Unread_Message",
            "description": "用户想要查看未读的消息",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Mini_App",
            "description": "按照小程序名字关闭相应小程序或应用。比如打开xxx，或打开xxx小程序。"
                           "关闭咪咕音乐、咪咕视频，请命中此函数",
            "parameters": {
                "type": "object",
                "properties": {
                    "Mini_app": {
                        "type": "string",
                        "description": "仅包括以下小程序，请提取出并映射为标准名字。"
                                       "小程序的标准名字有：宝宝巴士、滴滴代驾、白鲸鱼、凤凰FM、洗车、千里眼、熊猫天天讲故事、36氪、芒果动听、芒果TV、"
                                       "2048、1057、懒人听书、微博热搜、智慧酒店、智慧电影、智慧餐厅、智慧景点、沐耳FM、儿歌多多、智慧外卖、橙牛特惠洗车、"
                                       "咪咕音乐、咪咕视频、限行助手、咪咕体育、阿基米德FM"
                    }
                },
                "required": ["Mini_app"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Mini_App_Center",
            "description": "关闭小程序中心或小程序界面",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Mini_App",
            "description": "按照小程序名字打开相应小程序或应用。比如打开xxx，或打开xxx小程序。"
                           "或用户模糊表达想使用某小程序。比如：想点外卖-打开智慧外卖；想看微博发文-打开微博热搜；想找代驾-打开滴滴代驾"
                           "想订酒店-打开智慧酒店；想买门票-打开智慧景点；想买电影票-打开智慧电影"
                           "打开咪咕音乐或咪咕视频，请命中此函数",
            "parameters": {
                "type": "object",
                "properties": {
                    "Mini_app": {
                        "type": "string",
                        "description": "仅包括以下小程序，请提取出并映射为标准名字。"
                                       "小程序的标准名字有：宝宝巴士、滴滴代驾、白鲸鱼、凤凰FM、洗车、千里眼、熊猫天天讲故事、36氪、芒果动听、芒果TV、"
                                       "2048、1057、懒人听书、微博热搜、智慧酒店、智慧电影、智慧餐厅、智慧景点、沐耳FM、儿歌多多、智慧外卖、橙牛特惠洗车、"
                                       "咪咕音乐、咪咕视频、限行助手、咪咕体育、阿基米德FM"
                    }
                },
                "required": ["Mini_app"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Mini_App_Center",
            "description": "打开小程序中心或小程序界面",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Search_Music",
            "description": "播放音乐，而不是打开音乐。用户必须明确表达想听歌或听语音助手（你）唱歌，才能命中此函数",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Search_Radio",
            "description": "播放电台或在线电台，而不是打开电台。用户必须明确表达想听电台，才能命中此函数",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Search_Music",
            "description": "按照音乐排行榜、专辑、歌曲名称、歌手姓名、歌手地区、歌手类型、歌曲年代、歌曲主题、歌曲流派、歌曲语言、电影或电视剧、综艺节目、场景、车主心情、合唱的歌手、合唱的歌手加歌名、歌手加专辑、歌名加专辑、歌手加排除歌曲等去搜索或播放歌曲。或为我（用户）唱首xx的歌"
                           "不要把“我”、“你”、“他”、“她”等指代词提取为Singer和Singer2。"
                           "不能把参数提取为“不限”，若未指定以下参数，则设为空。"
                           "“xx（歌手的名字）有什么歌”的类似说法。不命中此函数"
                           "TV、variety_show和Name只能提取一个",
            "parameters": {
                "type": "object",
                "properties": {
                    "charts": {
                        "type": "string",
                        "description": "排行榜：如热门歌曲，经典怀旧榜，酷我真声音，英国UK Official，韩国M-net等。若未指定，则设为空，不能用不限."
                                       "不要随意提取改参数，只有确实存在该榜单时才能提取"
                    },
                    "album": {
                        "type": "string",
                        "description": "音乐专辑名字。若未指定，则设为空，不能用不限。若原句提到了专辑，就设为专辑。"
                                       "不能将歌手名字提取为album"
                    },
                    "Name": {
                        "type": "string",
                        "description": "歌曲名称，即歌曲。若未指定，则设为空，不能用不限",
                    },
                    "Singer": {
                        "type": "string",
                        "description": "歌手名字或合唱的第一位歌手名字。若未指定，则设为空，不能用不限。",
                    },
                    "Singer2": {
                        "type": "string",
                        "description": "合唱的第二位歌手名字。若未指定，则设为空，不能用不限。",
                    },
                    "location": {
                        "type": "string",
                        "description": "歌手地区，包括华语，欧美，日韩等。"
                                       "若未指定，则设为空，不能用不限",
                    },
                    "type": {
                        "type": "string",
                        "description": "歌手类型，只包括男、女、组合。如果与以上三类相关，请进行映射。若不属于以上三类，不提取type。"
                                       "若未指定，则设为空，不能用不限",
                    },
                    "Age": {
                        "type": "string",
                        "description": "歌曲年代。若未指定，则设为空，不能用不限",
                    },
                    "Language": {
                        "type": "string",
                        "description": "歌曲语言。若未指定，则设为空，不能用不限",
                    },
                    "Keywords": {
                        "type": "string",
                        "description": "歌曲关键字。和歌曲内容相关的关键内容。若未指定，则设为空，不能用不限",
                    },
                    "Style": {
                        "type": "string",
                        "description": "歌曲主题，比如治愈、灵魂、摇滚等。若未指定，则设为空，不能用不限",
                    },
                    "TV": {
                        "type": "string",
                        "description": "电影或电视剧，包括电视剧、电影等。如播放电视剧的主题曲。若未指定，则设为空，不能用不限"
                    },
                    "variety_show": {
                        "type": "string",
                        "description": "只包括综艺节目。如播放花儿与少年的歌。若未指定，则设为空，不能用不限"
                    },
                    "Mood": {
                        "type": "string",
                        "description": "和车主心情相反的情绪。比如用户现在有点难过，可以推荐治愈歌曲。若未指定，则设为空，不能用不限"
                    },
                    "Scene": {
                        "type": "string",
                        "description": "想在什么场景下听歌。比如运动时需要放一些激情歌曲。若未指定，则设为空，不能用不限。若未指明要听歌，不提取此参数"
                    },
                    "genre": {
                        "type": "string",
                        "description": "歌曲流派。电子，古典，爵士，流行，民歌，世界，嘻哈，摇滚，乡村等流派。若未指定，则设为空，不能用不限",
                    },
                    "exclude": {
                        "type": "string",
                        "description": "排除歌曲。除了xxx歌以外的歌曲。请把xxx提取为exclude",
                    }

                },
                "required": ["charts", "album", "Name", "Singer", "Singer2", "location", "Age", "Language", "Keywords", "Style", "TV", "variety_show", "Mood", "Scene", "genre", "exclude"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Play_Online_Music",
            "description": "播放在线音乐，而非本地音乐。在线音乐包括酷我音乐、",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Play_Similar_Music",
            "description": "退出相似歌曲或不再播放相似歌曲",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Switch_Music_Quantity",
            "description": "用户想切换音质，需要进行映射",
            "parameters": {
                "type": "object",
                "properties": {
                    "Quantity": {
                        "type": "string",
                        "description": "仅包括流畅、高品、超品、无损"
                    }
                },
                "required": ["Quantity"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Commute_Nav",
            "description": "关闭通勤导航或退出导航的通勤模式",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Commute_Nav",
            "description": "打开通勤导航或打开导航的通勤模式或激活通勤导航",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Cruise_Broadcast",
            "description": "关闭对前方路况的播报或查询",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Cruise_Broadcast",
            "description": "打开对前方路况的播报或查询前方路况",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Electronic_Eye",
            "description": "关闭电子眼或用户希望导航不再播报电子眼相关信息",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Electronic_Eye",
            "description": "打开电子眼或用户希望导航播报电子眼相关信息",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Safety_Alarm",
            "description": "关闭汽车里的安全提示",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Safety_Alarm",
            "description": "打开汽车里的安全提示",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Ask_Where",
            "description": "用户想知道或询问当前位置的具体地址",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Nav_Collections",
            "description": "关闭当前的收藏地址页面或导航点，即关闭导航收藏夹",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Nav_Collections",
            "description": "打开在导航里收藏夹页面，即用户想看自己收藏过哪些地址",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Collect_Current_Location",
            "description": "把当前所在地址放入导航收藏夹，即收藏当前地址",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Collect_Target_Location",
            "description": "把导航的目的地放入导航收藏夹，即收藏目的地地址",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Nav_To_Collection",
            "description": "导航去收藏夹中的地址",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Nav_To_Home",
            "description": "导航回家，即导航去设置为家的地址",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Nav_To_Company",
            "description": "导航去公司，即导航去设置为公司的地址",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Nav_Set_Home",
            "description": "用户想要把一个地址设置为家，即把默认家地址设为某地址。若用户只是想设置家地址，则不提取家POI。"
                           "如果输入中有第x个的说法，请将x提取为index。如果有附近的说法，不提取index"
                           "如果输入中有“确定”的含义。请将index提取为1",
            "parameters": {
                "type": "object",
                "properties": {
                    "家POI": {
                        "type": "string",
                        "description": "仅提取用户指定的具体的省市区县镇小区建筑等地址。"
                                       "如果地址是当前位置，请映射为当前位置；如果地址是目的地，请映射为目的地",
                    },
                    "index": {
                        "type": "string",
                        "description": "第x个，请提取为阿拉伯数字",
                    }
                },
                "required": ["家POI"]
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Nav_Set_Company",
            "description": "用户想要把一个地址设置为公司，即把公司默认地址设在某地址。若用户只是想设置公司地址，则不提取公司POI。"
                           "如果输入中有第x个的说法，请将x提取为index。如果有附近的说法，不提取index"
                           "如果输入中有“确定”的含义。请将index提取为1",
            "parameters": {
                "type": "object",
                "properties": {
                    "公司POI": {
                        "type": "string",
                        "description": "仅提取用户指定的具体的省市区县镇小区建筑等地址。"
                                       "如果地址是当前位置，请映射为当前位置；如果地址是目的地，请映射为目的地。",
                    },
                    "index": {
                        "type": "string",
                        "description": "第x个，请提取为阿拉伯数字",
                    }
                },
                "required": ["公司POI"]
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Nav",
            "description": "打开导航软件或用户上车准备出发了",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Switch_Main_Route",
            "description": "切换到主路大路或用户想走主路不想再走辅路",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Main_Route_First",
            "description": "用户希望导航优先规划走大路或主路的路线",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Switch_Side_Route",
            "description": "切换到辅路（辅道）或用户想走辅路（辅道）不想再走主路",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Change_Nav_Sign",
            "description": "切换导航标志",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Change_Nav_Sign",
            "description": "切换导航标志",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Head_Up",
            "description": "用户希望在导航视图里，车头一直保持朝上或朝北方向",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "2D_Map",
            "description": "车载导航地图设置为2D视图模式",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "3D_Map",
            "description": "车载导航地图设置为3D视图模式",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Map_Setting",
            "description": "关闭地图设置或者用户已经对地图设置完毕了",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Map_Setting",
            "description": "打开地图设置或者用户希望对地图进行设置",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Nav_Zoom_In",
            "description": "放大地图，但用户未指明到最大",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Nav_Zoom_In_Max",
            "description": "用户指定地图放到最大，即用户想要尽可能大的显示",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Nav_Zoom_Out",
            "description": "缩小地图，但用户未指明到最小",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Nav_Zoom_Out_Min",
            "description": "把地图缩到最小，即用户想要尽可能小",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "North_Up",
            "description": "让地图方向保持正北方向在上方",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Change_Route",
            "description": "用户想要换一条路线导航",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Front_Line_Detail",
            "description": "用户询问导航前方应该怎么走",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "View_Small_Map",
            "description": "用户想要查看小地图，即打开鹰眼图",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Small_Map",
            "description": "用户想要关闭小地图，即关闭鹰眼图",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Get_Route_Information",
            "description": "用户想要查看路线信息，如离目的地还有多远等",
            "parameters": {
                "type": "object",
                "properties": {
                    "Target": {
                        "type": "string",
                        "description": "用户指定的地址"
                    }
                }
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Back_Center",
            "description": "用户想要让导航视图回到以车为中心的视图，即回到自车位",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Nav_Broadcast",
            "description": "停止播报导航信息或用户不想听导航的播报内容了",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Full_Map",
            "description": "关闭对路线的全览视图",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Flush_Route",
            "description": "刷新路线，希望导航重新规划路线",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Nav_Broadcast",
            "description": "让导航播报导航信息或用户想听导航的播报内容了",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Nav",
            "description": "关闭导航或退出路线导航",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Full_Map",
            "description": "打开对路线的全览视图",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Replay_Broadcast",
            "description": "用户想再听一遍导航播报内容，即重复播报导航内容",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Map",
            "description": "用户希望设置地图的方向",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Traffic_Incidents",
            "description": "用户询问除自己的车以外有没有交通事故发生，即有没有车祸发生",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Ahead_Condition",
            "description": "查询沿途的路况，即在去POI的路上路况如何。路况仅包括交通堵塞情况、道路施工情况、路面状况、交通管制情况和路况预测情况。不包括沿途是否途经某一地点",
            "parameters": {
                "type": "object",
                "properties": {
                    "POI": {
                        "type": "string",
                        "description": "用户想要查询的地址"
                    }
                }
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Target_Condition",
            "description": "查询目的地处的路况",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Home_Condition",
            "description": "查询家附近或沿途的路况",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Company_Condition",
            "description": "查询公司附近或沿途的路况",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "POI_Condition",
            "description": "查询POI或POI附近的路况，而不是去POI路上的路况。路况仅包括交通堵塞情况、道路施工情况、路面状况、交通管制情况和路况预测情况，不包括路上会不会途经POI"
                           "如果是会不会途经，不命中此函数",
            "parameters": {
                "type": "object",
                "properties": {
                    "POI": {
                        "type": "string",
                        "description": "用户想要查询的POI，不包括家、公司、目的地等"
                    }
                }
            },
            "required": ["POI"]
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Avoid_Congestion",
            "description": "用户想走不拥堵的路线，想走顺畅的路线",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Cancel_Avoid_Congestion",
            "description": "用户可以走不拥堵的路线了",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Avoid_High_Way",
            "description": "用户不想走高速公路",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Cancel_Avoid_High_Way",
            "description": "用户可以走高速公路，即导航可以规划走高速的路线了",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Avoid_Limit_Line",
            "description": "用户想要避开限行路段",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Cancel_Avoid_Limit_Line",
            "description": "用户可以走限行路段，即导航可以规划经过限行路段的路线",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Cancel_High_Way_First",
            "description": "取消对导航设置优先走高速",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "High_Way_First",
            "description": "对导航设置优先走高速",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Cancel_Main_First",
            "description": "取消对导航设置优先走大路或主路",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Cancel_Avoid_Fee",
            "description": "取消导航对规划不用收费的路线的设置",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Avoid_Fee",
            "description": "让导航规划不用收费的路线",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Avoid_Fee",
            "description": "用户想让导航规划不用收费的路线",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Cancel_Smart_Recommend",
            "description": "用户取消或不想让导航推荐智能路线",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Smart_Recommend",
            "description": "用户想让导航推荐智能路线",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Cancel_Speed_Fast",
            "description": "用户不需要走耗时最短的路线了，即用户不赶时间了",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Speed_Fast",
            "description": "用户需要走耗时最短的路线，即用户很赶时间",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_AR_Nav",
            "description": "关闭AR导航，即将现实和虚拟结合引导车主的导航模式",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_AR_Nav",
            "description": "打开AR导航，即将现实和虚拟结合引导车主的导航模式",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Simple_Broadcast",
            "description": "关闭简洁播报，即打开导航的详细播报模式",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Simple_Broadcast",
            "description": "打开简洁播报，即关闭导航的详细播报模式",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Ask_Meeting_Place",
            "description": "用户想要设置集结地",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Build_Group",
            "description": "用户想要创建一个小队",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Go_Meeting_Place",
            "description": "用户现在要去集结地",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Group_Member_Location",
            "description": "想要查看小队里其他成员现在的位置",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Join_Group",
            "description": "用户希望加入一个车队",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Group",
            "description": "打开组队功能",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Quit_Group",
            "description": "退出组队模式",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Meeting_Place",
            "description": "设置具体的车队集结地址",
            "parameters": {
                "type": "object",
                "properties": {
                    "POI": {
                        "type": "string",
                        "description": "设置的车队集结的地址"
                    }
                }
            },
            "required": ["POI"]
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Add_Via",
            "description": "在去某地的路上要设置或新增途经点或询问路上（路边）有没有某途经点。若既没有指明某地也没有指明要途经，不命中此函数。"
                           "如果是先去Via，再去POI，也命中此函数。比如先去东方明珠再去人民公园，将Via提取为东方明珠，将POI提取为人民公园"
                           "如果输入中有第x个的说法，请将x提取为index。如果有附近的说法，不提取index"
                           "如果输入中有“确定”的含义。请将index提取为1",
            "parameters": {
                "type": "object",
                "properties": {
                    "POI": {
                        "type": "string",
                        "description": "要去的目的地。若未指定，则设为空。"
                                       "需要对地址进行规范，比如能买到打火机的地方-便利店、卖花的地方-花店。"
                                       "如果目的地是用户家，请映射为家；如果目的地是用户公司，请映射为公司"
                    },
                    "Via": {
                        "type": "string",
                        "description": "被设置为途经点的地址。需要对地址进行规范，比如能买到打火机的地方-便利店、卖花的地方-花店"
                    },
                    "index": {
                        "type": "string",
                        "description": "第x个，请提取为阿拉伯数字",
                    }
                }
            },
            "required": ["POI", "Via"]
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Delete_Via",
            "description": "用户想要删除途径点",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Search_News",
            "description": "按照新闻类型、新闻人物、地区、今日新闻、新闻关键字来搜索新闻（快讯）或播放新闻（快讯）。提取了Today就不要提取其他参数了"
                           "如果是介绍某位新闻人物，则不属于此类",
            "parameters": {
                "type": "object",
                "properties": {
                    "Type": {
                        "type": "string",
                        "description": "新闻类型，仅包括社会、热点、国际、财经、科技、体育、娱乐、军事、生活、教育、汽车、人文和旅游类。不属于以上类别则不提取Type"
                    },
                    "Key": {
                        "type": "string",
                        "description": "新闻关键字或新闻名称，如丝绸之路、昨天、新闻联播等"
                    },
                    "Person": {
                        "type": "string",
                        "description": "新闻人物",
                    },
                    "Area": {
                        "type": "string",
                        "description": "地区，如香港、叙利亚等",
                    },
                    "Today": {
                        "type": "string",
                        "description": "今日新闻或播放新闻或新闻快讯。如果明确不是今日，不命中此槽位，命中Key。如果是具体的日期，不命中此槽位，命中Key",
                    },
                },
                "required": ["Type", "Key", "Person", "Area", "Today"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Oil_Price",
            "description": "用户想要查询油价",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Play_OL_Radio",
            "description": "播放在线广播或用户想听在线广播。如果用户提到想听电台或想听在线电台，不命中此函数",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Radio_By_Name",
            "description": "按照指定的电台名称打开或切换到对应电台或广播",
            "parameters": {
                "type": "object",
                "properties": {
                    "Name": {
                        "type": "string",
                        "description": "电台名称"
                    }
                }
            },
            "required": ["Name"]
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Search_Radio",
            "description": "按照节目名称加主播姓名、电台类型、主播姓名、电台关键词、实时性、节目名称来搜索节目或播放电台节目，用于支持用户在车上收听。如果用户说“带我去”等说法，则不想在车上收听，不命中此函数。提取了Today就不要提取其他参数了。"
                           "或按照电视剧名字或其他节目名字播放节目。如果是播放某电视剧的主题曲，不要命中此函数"
                           "如果是介绍某位节目主播，则不属于此类。z"
                           "相声、小品、资讯、电视剧（或第xx集）等均属于节目。Key和Type只能提取一个。"
                           "如果用户表述播放xxx且输入中没有“哪个”等词语，只要xxx不属于歌曲、专辑和新闻，都命中此函数",
            "parameters": {
                "type": "object",
                "properties": {
                    "Type": {
                        "type": "string",
                        "description": "节目类型：如政治、科技、国际、人文等。或白话表示，比如适合小朋友的、适合老人听的等。"
                                       "不要把相声、小品、资讯、电台等提取为电台类型。若无则为空"
                    },
                    "Key": {
                        "type": "string",
                        "description": "电台关键词，比如丝绸之路、相声、小品、资讯、昨天等。若无则为空"
                    },
                    "Person": {
                        "type": "string",
                        "description": "主播姓名或相声主持人，如白岩松、郭德纲、郭德纲的徒弟等。若无则为空",
                    },
                    "Name": {
                        "type": "string",
                        "description": "节目名称或电视剧名称（可具体到第x季第x集），如香港之声、明朝那些事儿等。若无则为空",
                    },
                    "Today": {
                        "type": "string",
                        "description": "仅包括每日必听，播放电台，播放在线电台，想听电台，要听电台等。若无则为空",
                    },
                },
                "required": ["Type", "Key", "Person", "Name", "Today"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Play_Hot_Radio",
            "description": "播放今日热点、用户想听今日热搜等",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Search_Station",
            "description": "按照广播类型和广播名称搜索广播及广播频道",
            "parameters": {
                "type": "object",
                "properties": {
                    "Type": {
                        "type": "string",
                        "description": "广播类型：如政治、科技、国际等"
                    },
                    "Name": {
                        "type": "string",
                        "description": "广播名称",
                    },
                },
                "required": ["Type", "Name"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Speed_By_Speed",
            "description": "按照指定倍速播放",
            "parameters": {
                "type": "object",
                "properties": {
                    "speed": {
                        "type": "string",
                        "description": "播放倍速，比如0.5，1，1.5，2",
                    },
                },
                "required": ["speed"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Order_Center",
            "description": "关闭订单中心或交易界面",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Order_Center",
            "description": "打开订单中心或订单界面",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "View_All_Order",
            "description": "用户想要查看所有的订单或交易记录",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "View_Already_Order",
            "description": "用户想要查看已经完成的订单或交易记录",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "View_Unend_Order",
            "description": "用户想要查看还没完成的订单",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Record_Face",
            "description": "用户想要录入人脸进行人脸识别",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Personal_Center",
            "description": "关闭个人中心或个人信息界面",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Personal_Center",
            "description": "打开个人中心或个人信息界面",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "View_Call_Record",
            "description": "用户想要查看通话记录",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "View_Send_Record",
            "description": "查看用户给别人打电话的拨打记录",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "View_Not_Call",
            "description": "用户想要查看没接到的电话记录",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "View_Already_Call",
            "description": "用户想要查看已经接通的电话记录",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Get_Contact",
            "description": "用户想要查看联系人，或查看通讯录、或查看电话本",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Sync_Contact",
            "description": "用户想要同步联系人或更新通讯录",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Answer_Phone",
            "description": "接听电话，即接通来电",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Reject_Phone",
            "description": "拒接电话，即挂断还未接听的来电",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Silent_Phone",
            "description": "让来电铃声静音",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Phone",
            "description": "打开电话界面或拨号界面",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Call_Phone",
            "description": "用户需要打电话",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Call_Back",
            "description": "回电话，即回拨某号码",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Call_Phone_By_Contact",
            "description": "给指定联系人拨打电话",
            "parameters": {
                "type": "object",
                "properties": {
                    "Contact": {
                        "type": "string",
                        "description": "联系人，包括但不限于爸爸、妈妈等",
                    }
                },
                "required": ["Contact"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Call_Phone_By_Contact_Label",
            "description": "按照联系人和号码标签拨打电话，比如打给爸爸的家庭号码",
            "parameters": {
                "type": "object",
                "properties": {
                    "Contact":{
                        "type": "string",
                        "description": "联系人，包括但不限于爸爸、妈妈等"
                    },
                    "label": {
                        "type": "string",
                        "description": "标签包括但不限于工作、家庭等",
                    }
                },
                "required": ["Contact", "label"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Call_Emergency",
            "description": "拨打紧急电话，比如我要报警、拨打110等",
            "parameters": {
                "type": "object",
                "properties": {
                    "Emergency": {
                        "type": "string",
                        "description": "包括紧急电话的号码和标签。比如报警，即为110；救护车，即为120。若为标签，需要映射为号码",
                    }
                },
                "required": ["Contact"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Call_Number",
            "description": "拨打指定电话",
            "parameters": {
                "type": "object",
                "properties": {
                    "Phone_Number": {
                        "type": "string",
                        "description": "国内标准为11位数字组成的普通号码或7-8位数字组成的座机号码",
                    }
                },
                "required": ["Phone_Number"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Call_Yellow_Page",
            "description": "拨打黄页电话",
            "parameters": {
                "type": "object",
                "properties": {
                    "Yellow": {
                        "type": "string",
                        "description": "包括黄页电话的号码和标签。比如消费者投诉热线，即12315等",
                    }
                },
                "required": ["Yellow"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Call_Select_SubStr",
            "description": "按照号码段选择号码并拨打。比如：给我通讯录里尾号为8765的号码打电话",
            "parameters": {
                "type": "object",
                "properties": {
                    "SubStr": {
                        "type": "string",
                        "description": "号码子串，比如3329",
                    }
                },
                "required": ["SubStr"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Quit_Phone",
            "description": "退出电话界面或拨号界面",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Radio_Am",
            "description": "收听调幅广播或收音机",
            "parameters": {
                "type": "object",
                "properties": {
                    "AM": {
                        "type": "string",
                        "description": "调幅广播收听范围在在525KHz-1610KHz。比如AM525、五二五。如果说法为汉字，请将其转为数字",
                    }
                },
                "required": ["AM"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Radio_Fm",
            "description": "收听调频广播或收音机",
            "parameters": {
                "type": "object",
                "properties": {
                    "FM": {
                        "type": "string",
                        "description": "调频广播收听范围在在87-108MHz。比如FM105.6、幺零五点六。如果说法为汉字，请将其转为数字",
                    }
                },
                "required": ["FM"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Play_Local_Radio",
            "description": "播放收音机或本地电台的内容，特别注意:不是打开",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Play_Local_Radio",
            "description": "播放收音机或本地电台的内容，收听调幅广播，调频广播或收音机",
            "parameters": {
                "type": "object",
                "properties": {
                    "FM": {
                        "type": "string",
                        "description": "调频广播收听范围在在87-108MHz。比如FM105.6、幺零五点六。如果说法为汉字，请将其转为数字，若无则为空",
                    },
                    "AM": {
                        "type": "string",
                        "description": "调幅广播收听范围在在525KHz-1610KHz。比如AM525、五二五。如果说法为汉字，请将其转为数字，若无则为空",
                    }
                },
                "required": ["FM", "AM"]
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Scene_Center",
            "description": "关闭情景模式",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Scene_Center",
            "description": "打开情景模式",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Preset_Scene",
            "description": "退出用户指定的场景名称，需要进行映射",
            "parameters": {
                "type": "object",
                "properties": {
                    "Scene": {
                        "type": "string",
                        "description": "仅包括清凉模式、舒畅模式、甜蜜时光模式和温暖模式"
                    }
                },
                "required": ["Scene"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Snooze_Mode",
            "description": "关闭睡眠模式或休息模式",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Snooze_Mode_Fuzzy",
            "description": "睡醒了、睡好了等表达",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Preset_Scene",
            "description": "打开用户指定的场景名称，需要进行映射",
            "parameters": {
                "type": "object",
                "properties": {
                    "Scene": {
                        "type": "string",
                        "description": "仅包括清凉模式、舒畅模式、甜蜜时光模式和温暖模式"
                    }
                },
                "required": ["Scene"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Snooze_Mode",
            "description": "打开睡眠模式或休息模式，持续时长为用户指定时长",
            "parameters": {
                "type": "object",
                "properties": {
                    "Minute": {
                        "type": "string",
                        "description": "以分钟为单位的持续时长。若无则为空"
                    }
                }
            },
            "required": ["Minute"]
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Snooze_Mode_Fuzzy",
            "description": "用户想在车上休息或睡觉，或在停车时表达了自己很困",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_ScreenCast",
            "description": "关闭当前投屏或视频投射",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_ScreenCast",
            "description": "打开投屏或视频投射，即用户想用车载屏观看手机上的内容",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Get_Answer",
            "description": "用户想要修改为指定应答词，即用户希望语音助手（你）称呼用户（我）为某应答词",
            "parameters": {
                "type": "object",
                "properties": {
                    "Response": {
                        "type": "string",
                        "description": "应答词。由用户设置的对用户的称谓"
                    }
                },
                "required": ["Response"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Ask_Answer_Words",
            "description": "用户询问能否修改应答词，即用户（我）想要修改语音助手（你）称呼用户（我）的名字",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Delete_Answer_Words",
            "description": "用户希望删除应答词",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Voice_Style",
            "description": "将播报的语音风格切换为指定语音风格，需要进行映射",
            "parameters": {
                "type": "object",
                "properties": {
                    "Style": {
                        "type": "string",
                        "description": "包括但不限于简洁、标准、默认"
                    }
                },
                "required": ["Style"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Voice",
            "description": "将播报声音设置为指定声音。需要进行映射",
            "parameters": {
                "type": "object",
                "properties": {
                    "Voice": {
                        "type": "string",
                        "description": "包括暖心女声、清新女声、欢快女声、沉稳男声、可爱童声，如果不属于这五种，则映射为其他音色"
                    }
                },
                "required": ["Voice"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Change_Timbre",
            "description": "用户想切换播报音色，或觉得声音不好听",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Continuous_Dialogue",
            "description": "关闭连续对话功能，即用户不希望语音助手能和用户进行对话",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Continuous_Dialogue",
            "description": "打开连续对话功能，即用户希望语音助手能和用户进行对话",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Free_Wakeup",
            "description": "关闭免唤醒功能，即用户希望语音助手被他用唤醒词唤醒后再工作",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Free_Wakeup",
            "description": "打开免唤醒功能，即用户希望不用说出唤醒词就能让语音助手工作",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Interactive_Learning",
            "description": "关闭语音助手的交互学习功能",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Interactive_Learning",
            "description": "打开语音助手的交互学习功能",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Sds_Config",
            "description": "关闭语音设置界面",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Sds_Config",
            "description": "打开语音设置界面",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Skill_Instruction",
            "description": "关闭对语音技能的介绍",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Skill_Instruction",
            "description": "打开对语音技能的介绍",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Voice_Wakeup",
            "description": "关闭语音唤醒，即用户不希望通过语音来唤醒语音助手",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Voice_Wakeup",
            "description": "打开语音唤醒，即用户希望通过语音来唤醒语音助手",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Continuous_Dialogue",
            "description": "设置连续对话的时长",
            "parameters": {
                "type": "object",
                "properties": {
                    "Time": {
                        "type": "string",
                        "description": "以秒为单位的时长，比如10秒"
                    }
                },
                "required": ["Time"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Online_NLU",
            "description": "用户想停止使用在线语音",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Online_NLU",
            "description": "用户想使用在线语音",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Main_And_Vice",
            "description": "用户希望主副驾上的人均可唤醒语音助手",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Only_Main",
            "description": "用户希望只有主驾上的人才可唤醒语音助手",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Timbre_Child",
            "description": "用户希望把播报音色设置为童声",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Timbre_Default",
            "description": "用户希望把播报音色设置为默认音色",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Timbre_Female",
            "description": "用户希望把播报音色设置为女声",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Timbre_Male",
            "description": "用户希望把播报音色设置为男声",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "View_Learning_Content",
            "description": "用户希望查看语音助手交互学习的页面",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Accelerate_Speed",
            "description": "用户希望加快语音播报的速度",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Slow_Speed",
            "description": "用户希望放慢语音播报的速度",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Speech_By_Speed",
            "description": "按照指定倍速播报",
            "parameters": {
                "type": "object",
                "properties": {
                    "speed": {
                        "type": "string",
                        "description": "播报倍速，比如0.5，1，1.5，2",
                    },
                },
                "required": ["speed"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Ask_Wakeup_Words",
            "description": "用户询问能否修改唤醒词，或用户（我）想要重新给语音助手（你）取名字",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Delete_Wakeup_Words",
            "description": "用户希望删除唤醒词",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Wakeup_Words",
            "description": "用户想要把唤醒词修改为指定唤醒词，即用户（我）希望称呼语音助手（你）为某唤醒词",
            "parameters": {
                "type": "object",
                "properties": {
                    "Wake": {
                        "type": "string",
                        "description": "唤醒词。用户赋予的名字"
                    }
                },
                "required": ["Wake"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_App",
            "description": "用户希望关闭指定应用（包括游戏软件），并将指定应用提取为app，比如打开qq音乐、打开腾讯视频等。"
                           "如果用户说关闭咪咕音乐或咪咕视频，不命中此函数",
            "parameters": {
                "type": "object",
                "properties": {
                    "app": {
                        "type": "string",
                        "description": "指定app，比如QQ等"
                    }
                }
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_App",
            "description": "用户希望打开指定应用（包括游戏软件），并将指定应用提取为app，比如打开qq音乐、打开腾讯视频等。"
                           "如果用户说打开咪咕音乐或咪咕视频，不命中此函数",
            "parameters": {
                "type": "object",
                "properties": {
                    "app": {
                        "type": "string",
                        "description": "指定app，比如QQ等"
                    }
                },
                "required": ["app"]
            },
        },
    },

    {
        "type": "function",
        "function": {
            "name": "Close_App_List",
            "description": "用户希望关闭应用列表或关闭主菜单",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_App_List",
            "description": "用户希望打开应用列表或打开主菜单",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_System_Config",
            "description": "用户想要关闭某一项系统设置，即停用某一项系统设置的功能，其中系统设置项分为一级系统设置项和二级系统设置项。"
                           "One_Level和Two_level只能提取一个。",
            "parameters": {
                "type": "object",
                "properties": {
                    "One_Level": {
                        "type": "string",
                        "description": "一级系统设置项仅包括：连接，特色功能，系统，显示，智慧推荐"
                    },
                    "Two_Level": {
                        "type": "string",
                        "description": "二级系统设置项包括汽车及网络设置项。仅包括wifi、屏幕、声音、移动网络、蓝牙、亮度、热点等。请做映射"
                                       "但若不是汽车或网络设置项，则不命中此类。比如银行账户"
                    }
                },
                "required": ["One_Level", "Two_Level"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_System_Config",
            "description": "用户想要打开系统设置界面或某一项系统设置界面，其中系统设置项分为一级系统设置项和二级系统设置项.One_Level和Two_level只能提取一个。"
                           "优先提取二级系统设置",
            "parameters": {
                "type": "object",
                "properties": {
                    "One_Level": {
                        "type": "string",
                        "description": "一级系统设置项仅包括：连接，特色功能，系统，显示，智慧推荐"
                    },
                    "Two_Level": {
                        "type": "string",
                        "description": "二级系统设置项包括汽车及网络设置项。不限于wifi、屏幕、声音、移动网络、蓝牙、亮度、空调、壁纸等。"
                                       "但若不是汽车或网络设置项，则不命中此类。比如银行账户"
                    }
                },
                "required": ["One_Level", "Two_Level"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Dec_Brightness",
            "description": "按照数字把系统亮度调低某值，但不是调到最低，如果某值是一个区间，从区间随便取一个数即可。Number和Ratio只能提取一个。"
                           "若用户说小一点、够亮了等，将NUMBER提取为1，不提取RATIO",
            "parameters": {
                "type": "object",
                "properties": {
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                },
                "required": ["Number", "Ratio"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Inc_Brightness",
            "description": "按照数字把系统亮度或屏幕亮度调高调大，而不是调到某值，如果某值是一个区间，从区间随便取一个数即可。Number和Ratio只能提取一个",
            "parameters": {
                "type": "object",
                "properties": {
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                },
                "required": ["Number", "Ratio"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Brightness",
            "description": "按照数字把系统亮度或屏幕亮度调到某值，如果某值是一个区间，从区间随便取一个数即可。Number和Ratio只能提取一个",
            "parameters": {
                "type": "object",
                "properties": {
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                },
                "required": ["Number", "Ratio"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Brightness_Max",
            "description": "把系统亮度或屏幕亮度调到最高",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Brightness_Min",
            "description": "把系统亮度或屏幕亮度调到最低",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Card_Position",
            "description": "移动卡片到指定的卡片位置。需要进行映射",
            "parameters": {
                "type": "object",
                "properties": {
                    "Position": {
                        "type": "string",
                        "description": "卡片位置。必须映射到左侧或右侧"
                    }
                },
                "required": ["Position"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Flow",
            "description": "用户希望关闭流量",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Flow",
            "description": "用户希望打开流量",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Dec_DashBoard_Brightness",
            "description": "按照数字把仪表盘亮度调低某值，如果某值是一个区间，从区间随便取一个数即可。Number和Ratio只能提取一个",
            "parameters": {
                "type": "object",
                "properties": {
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                },
                "required": ["Number", "Ratio"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Inc_DashBoard_Brightness",
            "description": "按照数字把仪表盘亮度调高某值，如果某值是一个区间，从区间随便取一个数即可。Number和Ratio只能提取一个",
            "parameters": {
                "type": "object",
                "properties": {
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                },
                "required": ["Number", "Ratio"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Set_DashBoard_Brightness",
            "description": "按照数字把仪表盘亮度调到某值，但不是调到最大或最小，如果某值是一个区间，从区间随便取一个数即可。Number和Ratio只能提取一个",
            "parameters": {
                "type": "object",
                "properties": {
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                },
                "required": ["Number", "Ratio"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Set_DashBoard_Brightness_Min",
            "description": "把仪表盘亮度调到最暗",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Set_DashBoard_Brightness_Max",
            "description": "把仪表盘亮度调到最高",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Back_To_Home",
            "description": "用户想回到主页",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Spot",
            "description": "关闭热点",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Spot",
            "description": "打开热点",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Screen",
            "description": "关闭车载屏的大屏幕。若未指定屏幕，不命中此函数",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Screen",
            "description": "打开车载屏的大屏幕。若未指定屏幕，不命中此函数",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Phone_Connection",
            "description": "关闭或断开手机连接",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Phone_Connection",
            "description": "打开手机连接，即让车和手机进行连接或配对。与电话本和通讯录无关",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Set_System_Theme_By_Mode",
            "description": "按照用户指定模式设置系统主题",
            "parameters": {
                "type": "object",
                "properties": {
                    "mode": {
                        "type": "string",
                        "description": "用户指定模式，仅支持白天模式，黑夜模式和自动模式",
                    },
                },
                "required": ["mode"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Dec_Sound_Volume",
            "description": "按照声音来源和数字把相应音源的声音调低某值。若未指定声音来源就将声音来源设为空。Number和Ratio只能提取一个。"
                           "只有用户明确指定要调低音量，才命中此类",
            "parameters": {
                "type": "object",
                "properties": {
                    "Sound_Source": {
                        "type": "string",
                        "description": "声音来源，仅包括导航、电话、媒体、通知、语音和所有。若无则为空"
                    },
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                },
                "required": ["Sound_Source", "Number", "Ratio"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Inc_Sound_Volume",
            "description": "按照声音来源和数字把相应音源的声音调高某值。若未指定声音来源则把声音来源设为空。Number和Ratio只能提取一个。"
                           "只有用户明确指定要调高音量时，才命中此类",
            "parameters": {
                "type": "object",
                "properties": {
                    "Sound_Source": {
                        "type": "string",
                        "description": "声音来源，仅包括导航、电话、媒体、通知、语音和所有。若无则为空"
                    },
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                },
                "required": ["Sound_Source", "Number", "Ratio"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Sound_Volume",
            "description": "按照声音来源和数字把相应音源的声音调到某值，但不是调到最大或最小。若未指定声音来源就将声音来源设为空。Number和Ratio只能提取一个",
            "parameters": {
                "type": "object",
                "properties": {
                    "Sound_Source": {
                        "type": "string",
                        "description": "仅包括导航、电话、媒体、通知、语音和所有。若无则为空"
                    },
                    "Number": {
                        "type": "string",
                        "description": "提取出小数、负数、正整数，不提取百分数和分数。若无则为空"
                    },
                    "Ratio": {
                        "type": "string",
                        "description": "提取出百分数和分数，并将其表示为小数。只要含百分号就为百分数。若无则为空"
                    },
                },
                "required": ["Sound_Source", "Number", "Ratio"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Sound_Volume_Max",
            "description": "按照声音来源把相应音源的声音调到最高。若未指定声音来源就将声音来源设为空",
            "parameters": {
                "type": "object",
                "properties": {
                    "Sound_Source": {
                        "type": "string",
                        "description": "仅包括导航、电话、媒体、通知、语音和所有。若无则为空"
                    }
                },
                "required": ["Sound_Source"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Set_Sound_Volume_Min",
            "description": "按照声音来源把相应音源的声音调到最低，即静音或关闭声音。若未指定声音来源就将声音来源设为空。"
                           "若用户未提及声音，不命中此函数",
            "parameters": {
                "type": "object",
                "properties": {
                    "Sound_Source": {
                        "type": "string",
                        "description": "仅包括导航、电话、媒体、通知、语音和所有。若无则为空"
                    }
                },
                "required": ["Sound_Source"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Sound_Volume",
            "description": "按照声音来源把相应音源的声音打开，即解除静音。若未指定声音来源就将声音来源设为空",
            "parameters": {
                "type": "object",
                "properties": {
                    "Sound_Source": {
                        "type": "string",
                        "description": "仅包括导航、电话、媒体、通知、语音和所有。若无则为空"
                    }
                },
                "required": ["Sound_Source"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_WIFI",
            "description": "关闭wifi或无线网络",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_WIFI",
            "description": "打开wifi或无线网络",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Create_Trip_Record",
            "description": "用户想新建一条旅途记录",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Record_App",
            "description": "关闭途记这个软件",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Record_App",
            "description": "打开途记这个软件",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Play_USB_Music",
            "description": "用户想要播放USB音乐或U盘里的音乐",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Play_BT_Music",
            "description": "用户想要播放蓝牙音乐或手机音乐",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Temp_Compare",
            "description": "用户想要将某个城市的温度和该城市其他日期的温度做对比",
            "parameters": {
                "type": "object",
                "properties": {
                    "City": {
                        "type": "string",
                        "description": "城市名，如西安、南京"
                    }
                },
                "required": ["City"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Ask_Air_Condition",
            "description": "根据地点和时间查询空气质量或空气指标。地点和时间均可为空。请保持原句中表示时间的说法。smog和PM25只能提取一个或都不提取",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "若没有省市名，区县名等地点，则设为空；若有，则将其提取出来",
                    },
                    "date": {
                        "type": "string",
                        "description": "时间，若包含多个时间说法，请全部提取出来。比如下周三、国庆节、明天和后天、周三到周五等。"
                                       "如果有现在、此时此刻等说法，全部映射为”今天“",
                    },
                    "smog": {
                        "type": "string",
                        "description": "查询空气中是否有灰尘或烟雾",
                    },
                    "PM25": {
                        "type": "string",
                        "description": "查询空气中的PM2.5指数或浓度",
                    }
                },
                "required": ["location","date", "smog", "PM25"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Query_Weather",
            "description": "根据地点和时间查询天气，比如沙尘暴、雷电、大雨、冰雹、大雾、晴朗等天气。地点和时间均可为空。请保持原句中表示时间的说法。"
                           "sun、rain、snow、fog、quilt只能提取一个或都不提取",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "若没有省市名，区县名等地点，则设为空；若有，则将其提取出来。若用户指定地点为目的地，将其映射为目的地的说法",
                    },
                    "date": {
                        "type": "string",
                        "description": "时间，若包含多个时间说法，请全部提取出来。比如下周三、国庆节、明天和后天、周三到周五等。"

                    },
                    "sun": {
                        "type": "string",
                        "description": "询问天气是否晴朗，或是否有太阳"
                    },
                    "rain": {
                        "type": "string",
                        "description": "询问天气是否下雨，或是否需要带雨伞"
                    },
                    "snow": {
                        "type": "string",
                        "description": "询问天气是否下雪"
                    },
                    "fog": {
                        "type": "string",
                        "description": "询问天气是否有大雾或雾霾"
                    },
                    "quilt":{
                        "type": "string",
                        "description": "询问天气是否适合晒被子或是否需要收被子"
                    },
                },
                "required": ["location","date", "sun", "rain", "snow", "fog", "quilt"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Ask_Humidity",
            "description": "根据地点和时间查询湿度或干燥程度。地点和时间均可为空。请保持原句中表示时间的说法",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "若没有省市名，区县名等地点，则设为空；若有，则将其提取出来",
                    },
                    "date": {
                        "type": "string",
                        "description": "时间，若包含多个时间说法，请全部提取出来。比如下周三、国庆节、明天和后天、周三到周五等。"
                                       "如果有现在、此时此刻等说法，全部映射为”今天“",
                    }
                },
                "required": ["location","date"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Ask_Allergy",
            "description": "根据地点和时间查询过敏指数或是否容易过敏。地点和时间均可为空。请保持原句中表示时间的说法",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "若没有省市名，区县名等地点，则设为空；若有，则将其提取出来",
                    },
                    "date": {
                        "type": "string",
                        "description": "时间，若包含多个时间说法，请全部提取出来。比如下周三、国庆节、明天和后天、周三到周五等。"
                                       "如果有现在、此时此刻等说法，全部映射为”今天“",
                    }
                },
                "required": ["location","date"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Ask_Car_Wash",
            "description": "根据地点和时间查询洗车指数或是否适合洗车（刷车）。地点和时间均可为空。请保持原句中表示时间的说法",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "若没有省市名，区县名等地点，则设为空；若有，则将其提取出来",
                    },
                    "date": {
                        "type": "string",
                        "description": "时间，若包含多个时间说法，请全部提取出来。比如下周三、国庆节、明天和后天、周三到周五等。"
                                       "如果有现在、此时此刻等说法，全部映射为”今天“",
                    }
                },
                "required": ["location","date"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Ask_Clothes",
            "description": "根据地点和时间查询穿衣指数，或者是否适合穿厚衣服还是薄衣服。地点和时间均可为空。请保持原句中表示时间的说法。"
                           "dressing、scarf、hat、coat、t_shirt只能提取一个或都不提取",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "若没有省市名，区县名等地点，则设为空；若有，则将其提取出来",
                    },
                    "date": {
                        "type": "string",
                        "description": "时间，若包含多个时间说法，请全部提取出来。比如下周三、国庆节、明天和后天、周三到周五等。"
                                       "如果有现在、此时此刻等说法，全部映射为”今天“",
                    },
                    "dressing": {
                        "type": "string",
                        "description": "询问是否适合穿裙子",
                    },
                    "scarf": {
                        "type": "string",
                        "description": "询问是否适合戴围巾",
                    },
                    "hat": {
                        "type": "string",
                        "description": "询问是否适合戴帽子",
                    },
                    "coat": {
                        "type": "string",
                        "description": "询问是否适合穿外套",
                    },
                    "t_shirt": {
                        "type": "string",
                        "description": "询问是否适合穿短袖",
                    }
                },
                "required": ["location","date", "dressing", "scarf", "hat", "coat", "t_shirt"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Ask_Cold_Index",
            "description": "根据地点和时间查询感冒指数，或者是否容易感冒。地点和时间均可为空。请保持原句中表示时间的说法",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "若没有省市名，区县名等地点，则设为空；若有，则将其提取出来",
                    },
                    "date": {
                        "type": "string",
                        "description": "时间，若包含多个时间说法，请全部提取出来。比如下周三、国庆节、明天和后天、周三到周五等。"
                                       "如果有现在、此时此刻等说法，全部映射为”今天“",
                    }
                },
                "required": ["location","date"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Ask_Fishing",
            "description": "根据地点和时间查询钓鱼指数。地点和时间均可以为空。请保持原句中表示时间的说法",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "若没有省市名，区县名等地点，则设为空；若有，则将其提取出来",
                    },
                    "date": {
                        "type": "string",
                        "description": "时间，若包含多个时间说法，请全部提取出来。比如下周三、国庆节、明天和后天、周三到周五等。"
                                       "如果有现在、此时此刻等说法，全部映射为”今天“",
                    }
                },
                "required": ["location", "date"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Ask_Makeup",
            "description": "根据地点和时间查询化妆指数。地点和时间均可为空。请保持原句中表示时间的说法",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "若没有省市名，区县名等地点，则设为空；若有，则将其提取出来",
                    },
                    "date": {
                        "type": "string",
                        "description": "时间，若包含多个时间说法，请全部提取出来。比如下周三、国庆节、明天和后天、周三到周五等。"
                                       "如果有现在、此时此刻等说法，全部映射为”今天“",
                    }
                },
                "required": ["location","date"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Ask_Sport",
            "description": "根据地点和时间查询运动指数或者是否适合运动或散步或出门透气。地点和时间均可为空。请保持原句中表示时间的说法",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "若没有省市名，区县名等地点，则设为空；若有，则将其提取出来",
                    },
                    "date": {
                        "type": "string",
                        "description": "时间，若包含多个时间说法，请全部提取出来。比如下周三、国庆节、明天和后天、周三到周五等。"
                                       "如果有现在、此时此刻等说法，全部映射为”今天“",
                    }
                },
                "required": ["location","date"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Ask_Transport",
            "description": "根据地点和时间查询交通指数。地点和时间均可为空。请保持原句中表示时间的说法",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "若没有省市名，区县名等地点，则设为空；若有，则将其提取出来",
                    },
                    "date": {
                        "type": "string",
                        "description": "时间，若包含多个时间说法，请全部提取出来。比如下周三、国庆节、明天和后天、周三到周五等。"
                                       "如果有现在、此时此刻等说法，全部映射为”今天“",
                    }
                },
                "required": ["location","date"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Ask_Travel",
            "description": "根据地点和时间查询旅游指数。地点和时间均可为空。请保持原句中表示时间的说法",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "若没有省市名，区县名等地点，则设为空；若有，则将其提取出来",
                    },
                    "date": {
                        "type": "string",
                        "description": "时间，若包含多个时间说法，请全部提取出来。比如下周三、国庆节、明天和后天、周三到周五等。"
                                       "如果有现在、此时此刻等说法，全部映射为”今天“",
                    }
                },
                "required": ["location","date"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Query_Date",
            "description": "根据车牌号和城市查询什么时候限行。城市可为空，车牌号或车牌尾号如未指定，一定置为空",
            "parameters": {
                "type": "object",
                "properties": {
                    "City": {
                        "type": "string",
                        "description": "省市区县等地址"
                    },
                    "License": {
                        "type": "string",
                        "description": "车牌号或车牌尾号"
                    }
                },
                "required": ["City", "License"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Query_Restrictions",
            "description": "根据地点和时间查询限行车牌尾号或是否限行，或用户问汽车某时间能不能去某地点。地点和时间均可为空。请保持原句中表示时间的说法",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "若没有省市名，区县名等地点，则设为空；若有，则将其提取出来",
                    },
                    "date": {
                        "type": "string",
                        "description": "时间，若包含多个时间说法，请全部提取出来。比如下周三、国庆节、明天和后天、周三到周五等。"
                                       "如果有现在、此时此刻等说法，全部映射为”今天“",
                    }
                },
                "required": ["location","date"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Query_UV_Level",
            "description": "根据地点和时间查询紫外线强度或指数或是否需要戴墨镜。地点和时间均可为空。请保持原句中表示时间的说法。"
                           "sunscreen可以不提取",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "若没有省市名，区县名等地点，则设为空；若有，则将其提取出来",
                    },
                    "date": {
                        "type": "string",
                        "description": "时间，若包含多个时间说法，请全部提取出来。比如下周三、国庆节、明天和后天、周三到周五等。"
                                       "如果有现在、此时此刻等说法，全部映射为”今天“",
                    },
                    "sunscreen": {
                        "type": "string",
                        "description": "询问是否需要戴墨镜",
                    }
                },
                "required": ["location","date", "sunscreen"]
            },
        }
    },

    {
        "type": "function",
        "function": {
            "name": "Query_Body_Temperature",
            "description": "根据地点和时间查询某地某时的体感温度，即询问xx城市xx时间有多少度或温度多少。地点和时间均可为空。请保持原句中表示时间的说法",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "若没有省市名，区县名等地点，则设为空；若有，则将其提取出来",
                    },
                    "date": {
                        "type": "string",
                        "description": "时间，若包含多个时间说法，请全部提取出来。比如下周三、国庆节、明天和后天、周三到周五等。"
                                       "如果有现在、此时此刻等说法，全部映射为”今天“",
                    }
                },
                "required": ["location","date"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Ask_Wind",
            "description": "根据地点和时间查询风力大小或风向。时间和地点均可为空。请保持原句中表示时间的说法。"
                           "level和direction必须提取一个，并只能提取一个",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "若没有省市名，区县名等地点，则设为空；若有，则将其提取出来",
                    },
                    "date": {
                        "type": "string",
                        "description": "时间，若包含多个时间说法，请全部提取出来。比如下周三、国庆节、明天和后天、周三到周五等。"
                                       "如果有现在、此时此刻等说法，全部映射为”今天“",
                    },
                    "level": {
                        "type": "string",
                        "description": "用户查询风力大小",
                    },
                    "direction": {
                        "type": "string",
                        "description": "用户查询吹风方向，即风向",
                    }
                },
                "required": ["location","date", "level", "direction"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Query_Destination_Weather",
            "description": "查询目的地的天气。date可不提取",
            "parameters": {
                "type": "object",
                "properties": {
                    "date": {
                        "type": "string",
                        "description": "时间，若包含多个时间说法，请全部提取出来。比如下周三、国庆节、明天和后天、周三到周五等。"
                    }
                },
                "required": ["date"]
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Query_Timely_Weather",
            "description": "按照城市名来查询实时天气，即现在的天气。若未指定城市，则不提取location",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "若没有省市名，区县名等地点，则设为空；若有，则将其提取出来",
                    },
                },
                "required": ["location"]
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Weather",
            "description": "关闭天气软件或退出天气界面",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Weather",
            "description": "打开天气软件或进入天气界面",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Two_Both",
            "description": "同时打开两样东西",
            "parameters": {
                "type": "object",
                "properties": {
                    "One": {
                        "type": "string",
                        "description": "功能或物体"
                    },
                    "Two": {
                        "type": "string",
                        "description": "功能或物体"
                    }
                },
                "required": ["One", "Two"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Two_Both",
            "description": "同时关闭某些功能或物件",
            "parameters": {
                "type": "object",
                "properties": {
                    "One": {
                        "type": "string",
                        "description": "车内自带功能或硬件"
                    },
                    "Two": {
                        "type": "string",
                        "description": "车内自带功能或硬件"
                    }
                },
                "required": ["One", "Two"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "List_Repeat",
            "description": "用户希望循环或按顺序播放一个列表或歌单里的歌曲或内容",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Go_POI",
            "description": "用户想去目的地1或寻找（搜索/搜一下）目的地1或用户需要导航去目的地1，或去目的城市（区/县/镇）的目的地1。如果只出现了城市名，则只把城市名提取为POI。"
                           "或用户询问目的城市有没有（哪里有）目的地1。"
                           "或用户隐式表达想去某地。比如：想玩麻将-想去麻将馆；想看星星-想去天文馆；想喝提神咖啡-想去咖啡店；想出去转转-想去公园；有点不舒服-想去医院；我车好脏-想去洗车店"
                           "搜一下xxx或搜索xxx，其中xxx是地名或公司名或建筑或店铺商标或品牌名字"
                           "先去xxx再去xxx不命中此函数"
                           "如果输入中有第x个的说法，请将x提取为index。如果有附近的说法，不提取index"
                           "如果输入中有“确定”的含义。请将index提取为1",
            "parameters": {
                "type": "object",
                "properties": {
                    "POI": {
                        "type": "string",
                        "description": "目的地1，省市县镇等地址或建筑商铺或酒店住宿。需要进行地址规范。如能买油烟机的地方-油烟机专卖店、卖花的地方-花店、xxx住宿-xxx的酒店、星巴克、相亲的地方-婚介所。",
                    },
                    "City": {
                        "type": "string",
                        "description": "目的地1所在的城市（区/县/镇），若无则为空",
                    },
                    "index": {
                        "type": "string",
                        "description": "第x个，请提取为阿拉伯数字",
                    }
                },
                "required": ["POI", "City"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Go_POI",
            "description": "用户想去参照地附近的目的地，或离参照地最近的目的地，或参照地的目的地。不要提取City。"
                           "或者用户想去或询问参照地附近或里面能执行目的动作的目的地。请仔细提取目的地和参照地。凡是”xx附近“的表述，xx均为参照地，即Target。"
                           "如果输入中有第x个的说法，请将x提取为index。如果有附近的说法，不提取index"
                           "如果输入中有“确定”的含义。请将index提取为1",
            "parameters": {
                "type": "object",
                "properties": {
                    "POI": {
                        "type": "string",
                        "description": "目的地，省市县镇等地址或建筑商铺。需要进行地址规范。如能买油烟机的地方-油烟机专卖店、卖花的地方-花店、好吃的-餐厅、相亲的地方-婚介所"
                                       "或者是模糊表达目的地。比如询问有没有好吃的，就是想寻找餐厅。请对模糊表达的目的地也进行地址规范。比如：好吃的-餐厅。"
                                       "能执行目的动作的目的地",
                    },
                    "Target": {
                        "type": "string",
                        "description": "参照地，即”xx附近“表述或”xx的“表述里的xx。若未指明则为空。请将地址转为书面表达。如能买油烟机的地方-油烟机专卖店。"
                                       "如果参照地址是当前位置，请映射为当前位置；如果参照地址是用户家，请映射为家；"
                                       "如果参照地址是目的地，请映射为目的地；如果参照地址是用户公司，请映射为公司。"
                                       "参照地可以是路口、学校等地址",
                    },
                    "index": {
                        "type": "string",
                        "description": "第x个，请提取为阿拉伯数字",
                    }
                },
                "required": ["POI", "Target"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Ask_Date",
            "description": "用户询问今天是几号",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Ask_Weekday",
            "description": "用户询问今天是周几或星期几",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Calendar",
            "description": "关闭日历应用",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Calendar",
            "description": "打开日历应用",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Display_Remaining_Flow",
            "description": "用户想要查看剩余多少流量",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_APP_Rank",
            "description": "退出显示流量消耗排行榜的页面",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_APP_Rank",
            "description": "用户想看流量消耗排行榜，或用户询问什么软件耗费流量最多等",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Flow_Center",
            "description": "关闭流量中心或流量显示页面",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Flow_Center",
            "description": "打开流量中心或流量显示页面",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Long_Video",
            "description": "关闭长视频或优酷视频",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Long_Video",
            "description": "打开长视频或优酷视频，或用户想看电视剧，但未指定看哪一部电视剧",
            "parameters": {
                "type": "object",
                "properties": {
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Search_Music",
            "description": "搜索指定的儿童音乐或影视内容，需要进行映射。与音乐无关的，不命中此函数"
                           "如果输入中必须明确指定与儿童相关，才能命中此函数",
            "parameters": {
                "type": "object",
                "properties": {
                    "Content": {
                        "type": "string",
                        "description": "仅包括国学、儿童故事、胎教、儿歌、动画和早教。如果不属于以上六类，请不命中此函数，不包括作文"
                    }
                },
                "required": ["Content"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Go_Home",
            "description": "用户需要导航回家",
            "parameters": {
                "type": "object",
                "properties": {
                }
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Go_Company",
            "description": "用户需要导航去公司",
            "parameters": {
                "type": "object",
                "properties": {
                }
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Cruise_Information",
            "description": "打开当前路况信息",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Cruise_Information",
            "description": "关闭当前路况信息",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        },
    },

    {
        "type": "function",
        "function": {
            "name": "Unknown",
            "description": "无意义的，或未指明部件的命令，比如要左边的、关了吧。不包括确认、没错、取消等命令"
                           "如果输入需要搜索xx，则不命中此函数"
                           "一切和闲聊，询问歌曲演唱者、百科知识，娱乐知识，生活知识，诗词（含更换），旅游攻略，地理问题，数学运算，单位换算，旅游或音乐的推荐或建议，人物介绍，笑话，翻译等相关的问题。"
                           "包括去某地N天的旅行、出差等路书设计、路书中某天路线设计修改或途径点变更等。"
                           "如果仅是规划路线，则同导航去某地，不属于该类。"
                           "如果输入中有怎么、为什么等词语，必须命中此函数，不能命中其他函数",
            "parameters": {
                "type": "object",
                "properties": {}
            }
        },
    },
    {
        "type": "function",
        "function": {
            "name": "Unknown",
            "description": "用户询问某xx（歌名）是谁唱的或xx（歌名）的演唱者是谁，比如李白是谁唱的。"
                           "或用户询问xx（歌手的名字）有什么歌，比如李荣浩有什么歌",
            "parameters": {
                "type": "object",
                "properties": {}
            }
        },
    },
    {
        "type": "function",
        "function": {
            "name": "List_Select",
            "description": "用户说第Index个。你需要把Index提取出来",
            "parameters": {
                "type": "object",
                "properties": {
                    "Index": {
                        "type": "string",
                        "description": "正整数，请将其规范为阿拉伯数字"
                    }
                },
                "required": ["Index"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Confirm",
            "description": "确定、确认、是的、没错等。或用户确认具体动作，比如确认导航、确认前往、确认播放、确认拨打等"
                           "开始或开始导航命中此函数",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Cancel",
            "description": "取消、不去了的意思。或用户取消具体动作，比如取消导航，取消前往，取消拨打，取消播放等。不包括取消预约维保",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_K",
            "description": "用户想要打开唱歌软件（界面），或想唱歌但未指定想唱哪首歌。"
                           "或用户（我）想要自己唱歌，而不是让语音助手（你）唱歌",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_K",
            "description": "用户不想唱歌了，或要退出唱歌界面",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Select_Sing",
            "description": "用户想唱某位歌手的歌或唱某首歌。如果歌手和歌名都出现了，只需要提取歌名，不用提取歌手"
                           "只有当用户明确说明自己要唱时，才命中此函数。如果用户只是想听不唱，不命中此函数。"
                           "如果用户说，为我唱一首歌，不命中此函数",
            "parameters": {
                "type": "object",
                "properties": {
                    "Singer": {
                        "type": "string",
                        "description": "某位歌手的名字。"
                    },
                    "Song": {
                        "type": "string",
                        "description": "某首歌的歌名"
                    }
                },
                "required": ["Singer", "Song"]
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Check_Car_Condition",
            "description": "用户想要知道车辆状态是否有异常，或想要查看轮胎胎压、油耗、续航（剩余油量）、总里程数、维保计划（不包括预约维保或取消预约维保）。"
                           "Tire,gas,range,total,maintenance只能提取一个。如果用户只想知道车辆状态，则都不提取",
            "parameters": {
                "type": "object",
                "properties": {
                    "Tire": {
                        "type": "string",
                        "description": "若和轮胎胎压有关，设为1"
                    },
                    "gas": {
                        "type": "string",
                        "description": "若用户询问汽车油耗或百公里耗油量或平均耗油量，设为1"
                    },
                    "range": {
                        "type": "string",
                        "description": "若和汽车续航（剩余油量/需不需要去加油）有关，即用户询问还剩下多少汽油，或剩下的汽油还能跑多少公里。设为1"
                    },
                    "total": {
                        "type": "string",
                        "description": "若和汽车总行驶里程数有关，设为1"
                    },
                    "Maintenance": {
                        "type": "string",
                        "description": "若和汽车维保有关，即用户想知道汽车的保养计划或想知道下次保养在什么时候。设为1"
                    }
                },
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Bulter",
            "description": "用户想要打开车辆管家或车辆信息中心",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Bulter",
            "description": "用户想退出车辆管家，或不想再查看车辆信息",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Reserve_Maintenance",
            "description": "用户想要预约维保或打开维保预约中心",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Cancel_Reserve",
            "description": "用户想取消之前的维保预约",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Open_Training_Camp",
            "description": "用户想要打开语音训练营",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "Close_Training_Camp",
            "description": "用户想要关闭语音训练营",
            "parameters": {
                "type": "object",
                "properties": {}
            },
        }
    }
]
