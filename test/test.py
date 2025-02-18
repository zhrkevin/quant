#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------------------
# Copyright 2024 for Jingzhi & Level. All Rights Reserved.
# ---------------------------------------------------------

import random
import tkinter as tk


class DynamicNumberApp:

    def __init__(self, root):
        self.namespace = [
            '郑沅', '郑淳', '郑玺', '郑攸',
            '郑瑞泽', '郑瑞安', '郑舒扬', '郑若安', '郑予观',
            '郑炳恒', '郑颂祺', '郑聿吉', '郑书珩', '郑巨卿'
        ]
        self.root = root
        self.root.title("动态名字显示")

        # 创建当前显示标签
        self.name_label = tk.Label(root, text="当前名字:", font=("Helvetica", 32))
        self.name_label.pack()

        # 创建开始按钮
        self.start_button = tk.Button(root, text="开始", command=self.start_thread)
        self.start_button.pack()

        # 创建停止按钮
        self.stop_button = tk.Button(root, text="停止", command=self.stop_thread)
        self.stop_button.pack()

        self.dynamic_thread_running = False
        self.step = 1

    def start_thread(self):
        self.dynamic_thread_running = True
        self.update_gui()

    def stop_thread(self):
        self.dynamic_thread_running = False

    def update_gui(self):
        if self.dynamic_thread_running:
            name = random.choice(self.namespace)  # 随机选择一个名字
            self.name_label.config(text=f"当前名字: {name}\n")
            self.root.after(100, self.update_gui)


# 创建主窗口
root = tk.Tk()
root.geometry("300x200")

# 创建应用程序实例
app = DynamicNumberApp(root)

# 启动主循环
root.mainloop()
