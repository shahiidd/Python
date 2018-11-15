import win32com.client
from win32com.client import Dispatch
import datetime as date
import os.path
import cPickle

# Function to fetch messages and attachments for an outlook folder
def get_outlook_msgs(name,folder):
    msgs=[]
    errors=0
    for msg in folder.items:
        try:
            msg_datetime=(datetime(1970,1,1,0,0,0)+timedelta(0, 0, 0,1000*int(msg.SentOn))).strftime('%Y%m%d%H%M%S')
            msg_couter='%07d'%(hash(msg.Sender.Address+msg_datetime+msg.Subject)%2**20)
            attachment_names='|'.join([str(att.FileName) for att in msg.Attachments])
            msg_list=[]
            msg_list.append(msg_couter)
            msg_list.append(msg.SenderName)
            msg_list.append(msg.Sender.Address)
            msg_list.append(msg.TO)
            msg_list.append(msg.CC)
            msg_list.append(msg.BCC)
            msg_list.append(str(msg.SentOn))
            msg_list.append(msg.Subject)
            msg_list.append(msg.Body)
            msg_list.append(attachment_names)
            msgs.append(msg_list)
            for att in msg.Attachments:
                att_name=att.FileName
                att_ext=att_name[att_name.rfind('.')+1:]
                att_name=att_name[:att_name.rfind('.')]
                if not(os.path.exists(att_ext)):
                    os.mkdir(att_ext)
                att.SaveASFile('%s/%s/%s_%d_%s.%s'%(os.getcwd(),att_ext,att_name,msg_couter,msg_datetime,att_ext))
        except:
            errors+=1
    print 'Success: %d. Errors: %d'%(len(msg_list),errors)
    with open('msgs/%s'%name.replace('||','$'), 'wb') as output:
        cPickle.dump(msgs, output)

# Example call to the function
# get_outlook_msgs('root||archive_old||Inbox||Other||Training',outlook.Folders('archive_old').Folders('Inbox').Folders('Other').Folders('Training'))

if __name__ == "__main__":
    # Initiate a connection to outlook
    outlook = Dispatch("Outlook.Application").GetNamespace("MAPI")
    all_folders=[('root',outlook,0)]

    # Get a list of all the outlook folders and their contents
    print(all_folders)
    for name,folder,num_items in all_folders:
        print (name, folder, num_items)
        child_folders=[f.Name for f in folder.Folders]
        for child_folder in child_folders:
            all_folders.append((name+'||'+child_folder,folder.Folders(child_folder),len(folder.Folders(child_folder).items)))

    # Maintain a list of parsed folders
    if os.path.isfile('parse_folders.txt'):
        parsed_folders=open('parse_folders.txt','rb').read().splitlines()
        record=open('parse_folders.txt','ab')
    else:
        parsed_folders=[]
        record=open('parse_folders.txt','wb')

    for name,folder,num_items in all_folders:
        if num_items==0 or name in parsed_folders:
            continue
        print name
        get_outlook_msgs(name,folder)
        record.write(name+'\n')
        record.flush()
    record.close()