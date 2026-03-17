def make_date_tag(date):

    month = date.strftime("%b").upper()
    year = date.year
    return f"{month}{year}"
