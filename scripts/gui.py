#!/usr/bin/env python3

from tkinter import *
import sys
import json
from tkcalendar import Calendar
from datetime import date
from backend import *




CollateralPath="./DB_backups"
ETFListFile=CollateralPath+"/etf.list"
StockListFile=CollateralPath+"/stocks.list"
DBPath="./DBs"
InfoDBFileName=DBPath+"/info.db"
StockValueDBFileName=DBPath+"/stock_values.db"
ETFValueDBFileName=DBPath+"/etf_values.db"
EarningsDBFileName=DBPath+"/earnings.db"




"""
def notdone():  
    showerror('Not implemented', 'Not yet available') 
     
def makemenu(parent):
    menubar = Frame(parent)                        
    menubar.pack(side=TOP, fill=X)
    
    fbutton = Menubutton(menubar, text='File', underline=0)
    fbutton.pack(side=LEFT)
    file = Menu(fbutton)
    file.add_command(label='New...',  command=notdone,     underline=0)
    file.add_command(label='Open...', command=notdone,     underline=0)
    file.add_command(label='Quit',    command=parent.quit, underline=0)
    fbutton.config(menu=file)
     
    ebutton = Menubutton(menubar, text='Edit', underline=0)
    ebutton.pack(side=LEFT)
    edit = Menu(ebutton, tearoff=0)
    edit.add_command(label='Cut',     command=notdone,     underline=0)
    edit.add_command(label='Paste',   command=notdone,     underline=0)
    edit.add_separator()
    ebutton.config(menu=edit)
     
    submenu = Menu(edit, tearoff=0)
    submenu.add_command(label='Spam', command=parent.quit, underline=0)
    submenu.add_command(label='Eggs', command=notdone,     underline=0)
    edit.add_cascade(label='Stuff',   menu=submenu,        underline=0)
    return menubar
     
root = Tk()
for i in range(3):
    frm = Frame()  
    mnu = makemenu(frm)
    mnu.config(bd=2, relief=RAISED)
    frm.pack(expand=YES, fill=BOTH)
    Label(frm, bg='black', height=5, width=15).pack(expand=YES, fill=BOTH)
Button(root, text="Bye", command=root.quit).pack()
root.mainloop()





class Window(Frame):
    
    # Define settings upon initialization. Here you can specify
    def __init__(self, master=None):
        
        # parameters that you want to send through the Frame class. 
        Frame.__init__(self, master)   

        #reference to the master widget, which is the tk window                 
        self.master = master

        #with that, we want to then run init_window, which doesn't yet exist
        self.init_window()

    #Creation of init_window
    def init_window(self):

        # changing the title of our master widget      
        self.master.title("GUI")

        # allowing the widget to take the full space of the root window
        #self.pack(fill=BOTH, expand=1)

        # creating a menu instance
        menu = Menu(self.master)
        self.master.config(menu=menu)

        # create the file object)
        file = Menu(menu)

        # adds a command to the menu option, calling it exit, and the
        # command it runs on event is client_exit
        file.add_command(label="Exit", command=self.client_exit)

        #added "file" to our menu
        menu.add_cascade(label="File", menu=file)

        # create the file object)
        edit = Menu(menu)

        # adds a command to the menu option, calling it exit, and the
        # command it runs on event is client_exit
        edit.add_command(label="Undo")

        #added "file" to our menu
        menu.add_cascade(label="Edit", menu=edit)

    
    def client_exit(self):
        exit()

        
# root window created. Here, that would be the only window, but
# you can later have windows within windows.
root = Tk()

root.geometry("400x300")

#creation of an instance
app = Window(root).pack(fill="both", expand=True)

#mainloop 
root.mainloop()  



"""
sectorsjson="./DB_backups/sectors.json"
Sectors=[]
Industries=[]


def GetInfo(Sectors, Industries):
    d1={}
    with open(sectorsjson, "r") as fp:
        d1=json.load(fp)

    l1=list(d1.keys())
    Sectors+=l1
    l2=[]
    for elem in d1.keys():
        l2+=d1[elem]
    Industries+=l2

def quit():
    sys.exit()  


def notdone():  
    showerror('Not implemented', 'Not yet available') 

#def get_data():
#   print(ticker.get())


def get_ticker():
    toplevel = Toplevel()
    Label(toplevel, text = "Ticker:", fg="black").pack(side="left")
    ticker=StringVar()
    Entry(toplevel, textvariable=ticker, width=5).pack(side="left")
    def get_data():
        toplevel1 = Toplevel()
        text=Text(toplevel1, width=180, height=3)
        text.pack()
        #print(ticker.get())
        #print("here1")
        df=get_earnings_date(ticker.get(), InfoDBFileName)
        text.insert(END, str(df))
        #print("here2")
        toplevel.destroy()
    Button(toplevel, text = "OK", fg="red", command = get_data).pack()

def get_date():
    toplevel = Toplevel()
    s1=StringVar()
    
    def print_date():
        print(s1.get())
        toplevel.destroy()
    def show_cal():
        toplevel1 = Toplevel()
        def print_sel():
            s1.set(cal.selection_get()) 
            toplevel1.destroy() 
        today = str(date.today())
        l1=today.split("-")
        year=l1[0]
        month=l1[1]
        day=l1[2]
        cal = Calendar(toplevel1,
                    font="Arial 14", selectmode='day',
                    cursor="hand1", year=int(year), month=int(month), day=int(day))
        cal.grid(row=0,column=0)
        s1.set(cal.selection_get())  
        Button(toplevel1, text="Done", command=print_sel).grid(row=1, column=0) 
    L1 = Label(toplevel, text="Date")
    L1.grid(row=0, column=0)
    E1 = Entry(toplevel, bd =2, textvariable=s1)
    E1.grid(row=0, column=1)
    B1= Button(toplevel, text ="Cal", command = show_cal)
    B1.grid(row=0, column=2)
    Button(toplevel, text ="Ok", command = print_date).grid(row=1, column=1)


def get_dates():
    toplevel = Toplevel()
    s1=StringVar()
    s2=StringVar()
    
    def print_date():
        print("Start Date:",s1.get())
        print("End Date:",s2.get())
        toplevel.destroy()
    def show_cal1():
        toplevel1 = Toplevel()
        def print_sel():
            s1.set(cal.selection_get()) 
            toplevel1.destroy() 
        today = str(date.today())
        l1=today.split("-")
        year=l1[0]
        month=l1[1]
        day=l1[2]
        cal = Calendar(toplevel1,
                    font="Arial 14", selectmode='day',
                    cursor="hand1", year=int(year), month=int(month), day=int(day))
        cal.grid(row=0,column=0)
        s1.set(cal.selection_get())  
        Button(toplevel1, text="Done", command=print_sel).grid(row=1, column=0) 
    def show_cal2():
        toplevel1 = Toplevel()
        def print_sel():
            s2.set(cal.selection_get()) 
            toplevel1.destroy() 
        today = str(date.today())
        l1=today.split("-")
        year=l1[0]
        month=l1[1]
        day=l1[2]
        cal = Calendar(toplevel1,
                    font="Arial 14", selectmode='day',
                    cursor="hand1", year=int(year), month=int(month), day=int(day))
        cal.grid(row=0,column=0)
        s2.set(cal.selection_get())  
        Button(toplevel1, text="Done", command=print_sel).grid(row=1, column=0) 
    L1 = Label(toplevel, text="Start Date")
    L1.grid(row=0, column=0)
    E1 = Entry(toplevel, bd =2, textvariable=s1)
    E1.grid(row=0, column=1)
    B1= Button(toplevel, text ="Cal", command = show_cal1)
    B1.grid(row=0, column=2)
    L2 = Label(toplevel, text="End Date")
    L2.grid(row=1, column=0)
    E2 = Entry(toplevel, bd =2, textvariable=s2)
    E2.grid(row=1, column=1)
    B2= Button(toplevel, text ="Cal", command = show_cal2)
    B2.grid(row=1, column=2)
    Button(toplevel, text ="Ok", command = print_date).grid(row=2, column=1)


def settings():
    toplevel = Toplevel()
    cpath=StringVar()
    cpath.set(CollateralPath)
    Label(toplevel, text = "CollateralPath:", fg="black").grid(row=0, column=0)
    Entry(toplevel, textvariable=cpath, width=25).grid(row=0, column=1)
    def set_cpath():
        global CollateralPath
        CollateralPath=cpath.get()
        toplevel.destroy()
    Button(toplevel, text = "Change", fg="red", command = set_cpath).grid(row=0, column=2)
    efpath=StringVar()
    efpath.set(ETFListFile)
    Label(toplevel, text = "ETF List FileName:", fg="black").grid(row=1, column=0)
    Entry(toplevel, textvariable=efpath, width=25).grid(row=1, column=1)
    def set_efpath():
        global ETFListFile
        ETFListFile=efpath.get()
        toplevel.destroy()
    Button(toplevel, text = "Change", fg="red", command = set_efpath).grid(row=1, column=2)
    sfpath=StringVar()
    sfpath.set(StockListFile)
    Label(toplevel, text = "Stock List FileName:", fg="black").grid(row=2, column=0)
    Entry(toplevel, textvariable=sfpath, width=25).grid(row=2, column=1)
    def set_sfpath():
        global StockListFile
        StockListFile=sfpath.get()
        toplevel.destroy()
    Button(toplevel, text = "Change", fg="red", command = set_sfpath).grid(row=2, column=2)
    dbpath=StringVar()
    dbpath.set(DBPath)
    Label(toplevel, text = "DB Path:", fg="black").grid(row=3, column=0)
    Entry(toplevel, textvariable=dbpath, width=25).grid(row=3, column=1)
    def set_dbpath():
        global DBPath
        DBPath=dbpath.get()
        toplevel.destroy()
    Button(toplevel, text = "Change", fg="red", command = set_dbpath).grid(row=3, column=2)
    idbpath=StringVar()
    idbpath.set(InfoDBFileName)
    Label(toplevel, text = "Info DB FileName:", fg="black").grid(row=4, column=0)
    Entry(toplevel, textvariable=idbpath, width=25).grid(row=4, column=1)
    def set_idbpath():
        global InfoDBFileName
        InfoDBFileName=idbpath.get()
        toplevel.destroy()
    Button(toplevel, text = "Change", fg="red", command = set_idbpath).grid(row=4, column=2)
    evdbpath=StringVar()
    evdbpath.set(ETFValueDBFileName)
    Label(toplevel, text = "ETF Value DB FileName:", fg="black").grid(row=5, column=0)
    Entry(toplevel, textvariable=evdbpath, width=25).grid(row=5, column=1)
    def set_evdbpath():
        global ETFValueDBFileName
        ETFValueDBFileName=evdbpath.get()
        toplevel.destroy()
    Button(toplevel, text = "Change", fg="red", command = set_evdbpath).grid(row=5, column=2)
    svdbpath=StringVar()
    svdbpath.set(StockValueDBFileName)
    Label(toplevel, text = "Stock Value DB FileName:", fg="black").grid(row=6, column=0)
    Entry(toplevel, textvariable=svdbpath, width=25).grid(row=6, column=1)
    def set_svdbpath():
        global StockValueDBFileName
        StockValueDBFileName=svdbpath.get()
        toplevel.destroy()
    Button(toplevel, text = "Change", fg="red", command = set_svdbpath).grid(row=6, column=2)
    edbpath=StringVar()
    edbpath.set(EarningsDBFileName)
    Label(toplevel, text = "Earnings DB FileName:", fg="black").grid(row=7, column=0)
    Entry(toplevel, textvariable=edbpath, width=25).grid(row=7, column=1)
    def set_edbpath():
        global EarningsDBFileName
        EarningsDBFileName=edbpath.get()
        toplevel.destroy()
    Button(toplevel, text = "Change", fg="red", command = set_edbpath).grid(row=7, column=2)




def makemenu(parent):
    menubar = Frame(parent)                        
    menubar.pack(side=TOP, fill=X)
    
    #--------------------------

    fbutton = Menubutton(menubar, text='File', underline=0)
    fbutton.pack(side=LEFT)
    file = Menu(fbutton)
    file.add_command(label='Settings...',  command=settings,     underline=0)
    file.add_command(label='Quit',    command=parent.quit, underline=0)
    fbutton.config(menu=file)

    #--------------------------

    cbutton = Menubutton(menubar, text='Classification', underline=0)
    cbutton.pack(side=LEFT)
    classify = Menu(cbutton, tearoff=0)
    classify.add_separator()
    cbutton.config(menu=classify)

    submenu1 = Menu(classify, tearoff=0)
    for elem in Sectors:
        submenu1.add_command(label=elem, command=notdone,     underline=0)
    classify.add_cascade(label='Sectors',   menu=submenu1,        underline=0)

    submenu2 = Menu(classify, tearoff=0)
    for elem in Industries:
        submenu2.add_command(label=elem, command=notdone,     underline=0)
    classify.add_cascade(label='Industries',   menu=submenu2,        underline=0)
    
    #--------------------------

    ebutton = Menubutton(menubar, text='Earnings', underline=0)
    ebutton.pack(side=LEFT)
    earn = Menu(ebutton)
    earn.add_command(label='Stock...',  command=get_ticker,     underline=0)
    earn.add_command(label="Date...",    command=get_date, underline=0)
    earn.add_command(label="Date Range...",    command=get_dates, underline=0)
    ebutton.config(menu=earn)
    
    #--------------------------

    dbutton = Menubutton(menubar, text='DataBases', underline=0)
    dbutton.pack(side=LEFT)
    db = Menu(dbutton, tearoff=0)
    db.add_separator()
    dbutton.config(menu=db)

    submenu3 = Menu(db, tearoff=0)
    submenu3.add_command(label='Create', command=notdone,     underline=0)
    submenu3.add_command(label='Update', command=notdone,     underline=0)
    db.add_cascade(label='Earnings',   menu=submenu3,        underline=0)

    submenu4 = Menu(db, tearoff=0)
    submenu4.add_command(label='Create', command=notdone,     underline=0)
    submenu4.add_command(label='Update', command=notdone,     underline=0)
    db.add_cascade(label='Prices',   menu=submenu4,        underline=0)

    submenu5 = Menu(db, tearoff=0)
    submenu5.add_command(label='Create', command=notdone,     underline=0)
    submenu5.add_command(label='Update', command=notdone,     underline=0)
    db.add_cascade(label='Info',   menu=submenu5,        underline=0)

    #--------------------------

    sbutton = Menubutton(menubar, text='Strategies', underline=0)
    sbutton.pack(side=LEFT)
    strategy = Menu(sbutton, tearoff=0)
    strategy.add_separator()
    sbutton.config(menu=strategy)

    """
    submenu1 = Menu(classify, tearoff=0)
    for elem in Sectors:
        submenu1.add_command(label=elem, command=notdone,     underline=0)
    classify.add_cascade(label='Sectors',   menu=submenu1,        underline=0)

    submenu2 = Menu(classify, tearoff=0)
    for elem in Industries:
        submenu2.add_command(label=elem, command=notdone,     underline=0)
    classify.add_cascade(label='Industries',   menu=submenu2,        underline=0)
    """
    return menubar
    
def main(argv):
    l2 = []
    GetInfo(Sectors, Industries)
    Sectors.sort()
    Industries.sort()
    #print(Industries)

    MainWindow=Tk()
    MainWindow.geometry("1000x1000")
    MainWindow.title("TradeAnalyzer")
    #MainWindow.pack(fill=BOTH, expand=1)

    frm = Frame()  
    mnu = makemenu(frm)
    mnu.config(bd=2, relief=RAISED)
    frm.pack(expand=YES, fill=BOTH)


    MainWindow.mainloop()

"""
    menu=Menu(MainWindow)
    MainWindow.config(menu=menu)

    file=Menu(menu)
    file.add_command(label="Exit", command=quit)
    menu.add_cascade(label="File", menu=file)

    MainWindow.mainloop()


    b1 = Button(MainWindow, text="Exit", command=quit)
    b1.grid(column=0,row=0)

    var1=StringVar(MainWindow)
    var1.set("Sectors")

    w1=OptionMenu(MainWindow, var1, *Sectors)
    w1.grid(column=1, row=0)

    w2=Spinbox(MainWindow, values=Industries)
    w2.grid(column=2, row=0)
"""

    



if __name__ == "__main__":
    main(sys.argv[1:])      
 