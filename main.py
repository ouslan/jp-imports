from src.data.data_trans import DataTrans
# from src.visualization.graphs import Graphs
from src.features.downloader import Downloader
from simple_term_menu import TerminalMenu
from yaspin import yaspin

def main():
    main_menu_title = "  Main Menu.\n  Press Q or Esc to quit. \n"
    main_menu_items = ["Download Import data", "Process the data" "Graph", "Quit"]
    main_menu_cursor = "> "
    main_menu_cursor_style = ("fg_red", "bold")
    main_menu_style = ("bg_red", "fg_yellow")
    main_menu_exit = False

    main_menu = TerminalMenu(
        menu_entries=main_menu_items,
        title=main_menu_title,
        menu_cursor=main_menu_cursor,
        menu_cursor_style=main_menu_cursor_style,
        menu_highlight_style=main_menu_style,
        cycle_cursor=True,
        clear_screen=True,
    )
    while not main_menu_exit:
        main_sel = main_menu.show()
        if main_sel == 0:
            spin = yaspin(text='Downloading PR import data...', color='blue', spinner='dots')
            spin.start()
            Downloader().download_data(imports=True)
            spin.stop()
        elif main_sel == 1:
            spin = yaspin(text='procesing data...', color='blue', spinner='dots')
            spin.start()
            DataTrans(data_path='data/raw/import.csv').clean()
            spin.stop()
        elif main_sel == 6 or main_sel == None:
                main_menu_exit = True
if __name__ == "__main__":
    main()