def html_dynamic_table_generation(arg_table_headers, arg_table_data, arg_table_type=None):
    """ This function is responisble for generating the html table code based on input arguments. 
        currently only vertical table type is supported

    Args:
        arg_table_headers (List): table header column names list
        arg_table_data (2-dimesional list): table data in 2-dimensional list format
        arg_table_type (String): required table type


    Returns:
        String: html table code based on the required arguments
    """

    table_content = ''
    table_content = table_content + """    <table id="alter">    
        <tr>    """
    for eachcolumn in arg_table_headers:
        table_content = table_content + "               <th>" + eachcolumn + "</th>"    
    table_content = table_content + "    </tr>    "

    for EachList in arg_table_data:            
        table_content=table_content+'    <tr>    '    +'\n'
        for EachElem in EachList:
            if(str(EachElem) == "Fail"): 
                table_content=table_content+'            <td style="color:red;">'+ str( EachElem )+ '</td>    '    +'\n'
            else:
                table_content=table_content+'            <td>'+ str( EachElem )+ '</td>    '    +'\n'
            table_content=table_content+'        '    +'\n'
        table_content=table_content+'    </tr>    '    +'\n'
        table_content=table_content+'        '    +'\n'            
    table_content = table_content + """    </table><br>"""

    return table_content


def html_start_and_end_content_retrieve():

    start_tagg_details = """
    <html>    
        <head>    
        <style>    
        table, th, td {    
        border: 1px solid black;    
        border-collapse: collapse;    
        }    
        th, td {    
        }    
        table#alter tr:nth-child(even) {    
        background-color: #eee;    
        }    
        table#alter tr:nth-child(odd) {    
        background-color: #fff;    
        }    
        table#alter th {    
        color: white;    
        background-color: gray;    
        }    
            
            
        </style>    
        </head>    
    <body>Hi,<br>"""
    end_tagg_details = """<br><br>Note: This is Auto-generated email. <br><br>Regards,<br>Data Validation Tool</body></html>"""

    return start_tagg_details, end_tagg_details