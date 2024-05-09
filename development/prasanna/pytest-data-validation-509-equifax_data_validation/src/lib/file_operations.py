def xml_retrieve_required_tag_value(arg_req_file, arg_required_tag):
    f=open(arg_req_file, "r")
    file_content =f.read().splitlines()
    f.close()
    for eachline in file_content:
        if ("<" + arg_required_tag + ">") in eachline:
            strt_indx = eachline.index("<" + arg_required_tag + ">") + len("<" + arg_required_tag + ">")
            end_indx = eachline.index("</" + arg_required_tag + ">")
            req_content = eachline[strt_indx:end_indx]
            return req_content
    return False


def text_file_retrieve_file_content_fun(arg_req_file, arg_read_type=None):
    f=open(arg_req_file, "r")
    file_content =f.read()
    if arg_read_type == "linebyline":
        file_content = file_content.splitlines()
    f.close()
    return file_content


def text_file_creation_fun(arg_file_name, arg_file_content):
    """this function will be responsible for creating the text file 
    Args:
        arg_file_name (string): required text file name with full path
        arg_file_content (string): required content to be added to text file 
    Returns:
        Boolean: True by default
    """
    f= open(arg_file_name,"w+")    
    f.write(str(arg_file_content))
    f.close()
    return True