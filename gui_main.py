import sys
from PyQt6.QtWidgets import QApplication
from gui.main_window import FasihGui

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion") 
    win = FasihGui()
    win.show()
    sys.exit(app.exec())