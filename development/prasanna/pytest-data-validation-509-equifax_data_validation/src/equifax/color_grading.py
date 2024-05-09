import math
def highlight_scope_creep(val):
    if val == '':
        color = 'yellow'
        return f'background-color: {color}'
    else:
        color = 'red' if int(val) < 20 else 'limegreen'
        return f'background-color: {color}'


def highlight_commit_done(val):
    if val == '':
        color = 'yellow'
        return f'background-color: {color}'
    else:
        color = 'red' if int(val) < 70 else 'limegreen'
        return f'background-color: {color}'


def highlight_predicability_range(val):
    if val == '':
        color = 'yellow'
        return f'background-color: {color}'
    else:
        if math.isnan(val):
            val = 0
        color = 'red' if int(val) < 30 else 'limegreen'
        return f'background-color: {color}'


def highlight_dfreq(val):
    if val == '':
        color = 'yellow'
        return f'background-color: {color}'
    else:
        color = 'red' if float(val) < 4 else 'limegreen'
        return f'background-color: {color}'


def highlight_leadtime(val):
    if val == '':
        color = 'yellow'
        return f'background-color: {color}'
    else:
        color = 'red' if float(val) < 21 else 'limegreen'
        return f'background-color: {color}'


def highlight_cfr(val):
    if val == '':
        color = 'yellow'
        return f'background-color: {color}'
    else:
        color = 'red' if float(val) < 5 else 'limegreen'
        return f'background-color: {color}'


def highlight_mttr(val):
    if val == '':
        color = 'yellow'
        return f'background-color: {color}'
    elif val == "No Incident Found":
        color = 'limegreen'
        return f'background-color: {color}'
    else:
        color = 'red' if float(val) > 2 else 'limegreen'
        return f'background-color: {color}'


def style_negative_red(val):
    if val <= 0:
        color = "lightcoral"
        return f'background-color: {color}'
    else:
        return ''
