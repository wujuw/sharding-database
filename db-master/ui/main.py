# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'main.ui'
#
# Created by: PyQt5 UI code generator 5.13.2
#
# WARNING! All changes made in this file will be lost!

import os
import image
import images
import result
import sys
# import ../master.py
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../")
import master
import sys
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtWidgets import QApplication,QMainWindow


class Ui_DistributedDatabase(object):
    def setupUi(self, DistributedDatabase):
        self.master = master.Master()
        DistributedDatabase.setObjectName("DistributedDatabase")
        DistributedDatabase.setWindowModality(QtCore.Qt.WindowModal)
        DistributedDatabase.resize(843, 652)
        font = QtGui.QFont()
        font.setPointSize(18)
        DistributedDatabase.setFont(font)
        self.centralwidget = QtWidgets.QWidget(DistributedDatabase)
        self.centralwidget.setObjectName("centralwidget")
        self.contentlineedit = QtWidgets.QLineEdit(self.centralwidget)
        self.contentlineedit.setGeometry(QtCore.QRect(60, 200, 700, 71))
        font = QtGui.QFont()
        font.setPointSize(12)
        self.contentlineedit.setFont(font)
        self.contentlineedit.setObjectName("contentlineedit")
        self.label = QtWidgets.QLabel(self.centralwidget)
        self.label.setGeometry(QtCore.QRect(260, 120, 291, 61))
        font = QtGui.QFont()
        font.setPointSize(18)
        font.setBold(True)
        font.setWeight(75)
        self.label.setFont(font)
        self.label.setStyleSheet("color: rgb(255, 255, 255);\n"
"background-color: rgb(221, 107, 079);")
        self.label.setAlignment(QtCore.Qt.AlignCenter)
        self.label.setObjectName("label")
        self.pushButton = QtWidgets.QPushButton(self.centralwidget)
        self.pushButton.setGeometry(QtCore.QRect(330, 410, 141, 71))
        self.pushButton.setStyleSheet("background-color: rgb(75, 173, 238);\n"
"border-style: outset;\n"
"border-width: 1px;\n"
"border-radius:16px;\n"
"font: bold 16px;\n"
"min-width:2em;\n"
"color:white;")
        self.pushButton.setObjectName("pushButton")
        self.pushButton.clicked.connect(self.getcontent)
        self.listView = QtWidgets.QListView(self.centralwidget)
        self.listView.setGeometry(QtCore.QRect(-5, -9, 851, 631))
        self.listView.setStyleSheet("background-image: url(:/newPrefix/lib1.jpg);")
        self.listView.setObjectName("listView")
        self.listView.raise_()
        self.contentlineedit.raise_()
        self.label.raise_()
        self.pushButton.raise_()
        DistributedDatabase.setCentralWidget(self.centralwidget)
        self.menubar = QtWidgets.QMenuBar(DistributedDatabase)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 843, 26))
        self.menubar.setObjectName("menubar")
        DistributedDatabase.setMenuBar(self.menubar)
        self.statusbar = QtWidgets.QStatusBar(DistributedDatabase)
        self.statusbar.setObjectName("statusbar")
        DistributedDatabase.setStatusBar(self.statusbar)

        self.retranslateUi(DistributedDatabase)
        QtCore.QMetaObject.connectSlotsByName(DistributedDatabase)

    def retranslateUi(self, DistributedDatabase):
        _translate = QtCore.QCoreApplication.translate
        DistributedDatabase.setWindowTitle(_translate("DistributedDatabase", "MainWindow"))
        self.label.setText(_translate("DistributedDatabase", u"XXX大学图书馆系统"))
        self.pushButton.setText(_translate("DistributedDatabase", u"搜索"))
        self.pushButton.setProperty("class", _translate("DistributedDatabase", "black"))

    def getcontent(self):
        sql = self.contentlineedit.text()
        res = self.master.execute(sql)
        mainWindow = QMainWindow()
        ui = result.Ui_MainWindow()
        ui.setupUi(mainWindow)
        ui.fill_data(sql= sql, res= res)
        mainWindow.show()
        # print(text)

if __name__=='__main__':
    app = QtWidgets.QApplication(sys.argv)
    Window = QMainWindow()  # 注意窗口类型
    ui = Ui_DistributedDatabase()
    ui.setupUi(Window)
    Window.show()
    sys.exit(app.exec_())