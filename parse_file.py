import os
import re

import unidecode


def replace_table(table):
    """Decides whether to keep a table.

    Strips the table of its HTML, then compares the number of alphabet
    characters to numerical characters. If the table is over 15% numbers,
    or if the table contains 'Item 7' or 'Item 8', we should remove the table
    entirely. Otherwise, we return the table, still stripped of HTML.

    Args:
        table (str): the table tag and its contents to be analyzed

    Returns:
        str: either the original table stripped of html, or ''
    """
    table = re.sub("<[^<]+?>", "", table)  # remove all html tags
    # Find number of digits and letters in string
    num = sum(char.isdigit() for char in table) * 1.0
    alph = sum(char.isalpha() for char in table) * 1.0
    if (
        (num + alph) > 0
        and num / (num + alph) > 0.15
        and "Item 7" not in table
        and "Item 8" not in table
    ):
        return ""
    return table


def extract_header(header):
    """Extracts information important for finding company name from the header and removes the rest.

    The function first removes everything before the COMPANY DATA section. Then it checks whether there is a FORMER
    COMPANY section in the header. If so it removes all other information besides this section and the COMPANY DATA
    section. Otherwise it removes everything except for the COMPANY DATA section.

     Args:
         header (str): the header to be processed
     Returns:
         str: the header with only company name and former name left in
    """
    header = re.sub(r".*?COMPANY DATA:", "", header, flags=re.S)
    if "FORMER COMPANY" in header:
        header = re.sub(
            r"FILING VALUES:.*BUSINESS PHONE:", "", header, flags=re.U | re.S
        )

    else:
        header = re.sub(r"FILING VALUES:.*", "", header, flags=re.S)

    return header


def strip_html(file):
    """Parses the HTML text, leaving only relevant data.

    Args:
        file (str): the text to be parsed.
    Returns:
        str: the cleaned text.
    """
    # Steps 1-4
    # A list of regular expressions that identify specific opening and closing tags with any number of attributes.
    ascii_encoded_segments = [
        r"<document>\n<type>graphic.*?</document>",
        r"<document>\n<type>zip.*?</document>",
        r"<document>\n<type>excel.*?</document>",
        r"<document>\n<type>pdf.*?</document>",
        r"<document>\n<type>xml.*?</document>",
        r"<PDF>.*?</PDF>",
    ]
    formatting_tags = [r"<div.*?>", r"</div>", r"<tr.*?>", r"</tr>"]
    formatting_tags_nolinefeed = [r"<td.*?>", r"</td>", r"<font.*?>", r"</font>"]
    xbrl = [r"<XBRL.*?>.*?</XBRL>"]
    end_message = [r"-----end privacy-enhanced message-----"]
    header_footer = [r"<SEC-FOOTER.*?/SEC-FOOTER>"]
    file = re.sub(
        r"<SEC-HEADER>.*/SEC-HEADER>",
        lambda p: extract_header(p.group()),
        file,
        flags=re.S,
    )
    file = re.sub(
        r"<IMS-HEADER.*/IMS-HEADER>",
        lambda p: extract_header(p.group()),
        file,
        flags=re.S,
    )

    # List of tags to remove
    regexps = (
        ascii_encoded_segments + xbrl + formatting_tags + end_message + header_footer
    )
    # Remove all instances of those regular expressions
    for r in regexps:
        # re.I = case-insensitive, re.S = '.' matches any character including newlines
        file = re.sub(r, r"\n", file, flags=re.I | re.S)
    for r in formatting_tags_nolinefeed:
        file = re.sub(r, " ", file, flags=re.I | re.S)
    file = re.sub(
        r"\s+(?=[:,;\.])", "", file
    )  # Remove all spaces that occur before a . , ; :
    # Remove tables based on their ratio of numbers to letters
    file = re.sub(
        r"<TABLE.*?/TABLE>|<table.*?/table>",
        lambda t: replace_table(t.group()),
        file,
        flags=re.I | re.S,
    )
    # Step 5
    # Remove all html tags EXCEPT document

    file = re.sub(
        "<(?!document|/document|<|type).*?>", " ", file, flags=re.I | re.S
    )  # Step 6
    char_mapping = {
        "&LT;": "<",
        "&#60;": "<",
        "&GT;": ">",
        "&#62;": ">",
        "&NBSP;": " ",
        "&#160;": " ",
        "&QUOT;": '"',
        "&#34;": '"',
        "&APOS;": "'",
        "&RSQUO;": "'",
        "&LDQUO;;": "'",
        "&#39;": "'",
        "&AMP;": "&",
        "&#38;": "&",
    }
    file = re.sub(
        r"&.*?;",
        lambda c: char_mapping[c.group().upper()]
        if c.group().upper() in char_mapping
        else c.group(),
        file,
        flags=re.I,
    )  # Step 7a-f, re-encode reserved HTML characters

    file = unidecode.unidecode(file)  # Step 7h, remove the rest of the ISO elements
    file = re.sub(r"&#.*?;", "", file)  # Step 7h, remove the rest of the ISO elements
    file = re.sub(r"-\n", "-", file)  # Step 8a, linefeeds following hyphens removed
    file = re.sub(
        r"\s-", " ", file
    )  # Step 8b, hyphens surrounded by blank space removed
    file = re.sub(
        r"-\s", " ", file
    )  # Step 8b, hyphens surrounded by blank space removed
    file = file.replace("_", " ")  # Step 8e, underscore characters removed
    file = re.sub(r" +", " ", file)  # Step 8f, many consecutive blanks are eliminated
    file = re.sub(
        r"(\n\s*){3,}", "\n\n", file
    )  # Step 8g, many consecutive linefeeds are eliminated
    file = re.sub(r"(?<=[^\n])\n(?=[^\n\s])", " ", file)  # Step 8h, reduce linefeeds
    file = re.sub(
        r"(?<=\n)\s*[0-9]+(?=\s*\n)", "", file
    )  # Step 16, remove page numbers
    file = re.sub(
        r"\s*\n+\s*\n*(?=[a-z])", " ", file
    )  # Step 16, make pretty after removing page numbers
    file = re.sub(
        r"(?<=[a-z])\s*\n+\s*\n*", " ", file
    )  # Step 16, make pretty after removing page numbers
    nums = re.findall(
        r"(\b[0-9]{1,3}(,[0-9]{3})*(\.[0-9]+)?\b|\.[0-9]+\b)", file, flags=re.I | re.M
    )
    for num in nums:
        file = file.replace(num[0], num[0].strip("."))
        file = file.replace(num[0], num[0].strip(","))

    file = re.sub(r"--+|\.\.+|==+", "", file)
    # Step 8d, sequences of two or more hyphens, periods, or equal signs are removed

    # Replace <TYPE>EX# with <EX#> tag and </EX#> tag
    lines = file.splitlines()
    new_file = ""
    closing_tag = ""
    for _, line in enumerate(lines):
        line = line + "\n"
        if "</DOCUMENT>" in line:
            # Replace the closing document tag with closing exhibit tag
            line = line.replace("</DOCUMENT>", closing_tag)
        if "<DOCUMENT>" in line:
            line = line.replace("<DOCUMENT>", "")
        if "<TYPE>EX" in line:

            # Extract the exhibit number from the text
            old_ex = re.findall(
                r"<TYPE>EX.*?(?=[\n<\s])", line, flags=re.I | re.S | re.M
            )[0]
            exhibit = old_ex.replace("<TYPE>", "")
            # Create the new tags
            line = line.replace(old_ex, "\n<{}>".format(exhibit))
            closing_tag = "</{}>".format(exhibit)
        new_file += line

    # Puts all-caps titles on the same line in the case that they were split up
    new_file = re.sub(r"(?<=[A-Z])\n (?=[A-Z])", " ", new_file)
    new_file = re.sub(
        r"(?<=[0-9])\s*(?=\))", "", new_file
    )  # Rejoins parenthases to numbers
    new_file = re.sub(
        r"(?<=\$)\s*(?=[0-9]|\()", "", new_file
    )  # Rejoins dollar signs to numbers
    new_file = re.sub(
        r"(?<=[0-9])\s*(?=%)", "", new_file
    )  # Rejoins percent signs to numbers
    new_file = re.sub(r"\s+\$\s+", "\n", new_file)  # Removes floating dollar signs
    new_file = re.sub(
        r"\s+\$\s+", "\n", new_file
    )  # Replace the ones it missed after the replacement
    new_file = re.sub(
        r" +(?=\.)", "", new_file
    )  # Removes spaces between periods and their sentences
    new_file = re.sub(r"(?<=\n) *", "", new_file)  # Removes leading spaces
    new_file = re.sub(r"- [- ]+", "", new_file)  # Removes remaining dashed lines
    new_file = re.sub(r"&ldquo; *| *&rdquo;", "'", new_file)

    return new_file


def parse(of, path, file):
    """Loads in the file from the specified path, parses the file, and saves it to a .txt file.

    Args:
        of (str): the original folder name that is appended to the new folder name so it is of_parsed.
        path (str): the path where the file is located.
        file (str): the name of the file to be parsed and saved.
    """
    # Create output path if it does not yet exist
    output_path = "parsed_" + of.replace("/", "") + "/" + (path.replace(of, ""))
    if not os.path.exists(output_path):
        os.makedirs(output_path)
        print("Path created: {0}".format(output_path))
    existing_items = os.listdir(output_path)
    f_out_name = "parsed_" + file
    if f_out_name not in existing_items:
        f_out = open(output_path + "parsed_" + file, "w")
        with open(path + file, "r", encoding="UTF-8", errors="ignore") as f_in:
            doc = f_in.read()
            doc_parsed = strip_html(doc)
            f_out.write(doc_parsed)
            f_out.close()


def parse_recursive(of, folder_path):
    """
    Iterate through the given folder, parsing .txt files and saving results to a 'parsed' folder.

    Args:
        of (str): same path as folder_path, keeps a copy of the original folder name so that
        when the function stops recursing it can create a folder with the original name.
        folder_path (str): path to the folder containing the files to be parsed.
    """
    items = os.listdir(folder_path)
    for item in items:
        # If the item is a directory that isn't called 'parsed'
        if (
            os.path.isdir(folder_path + item)
            and "parsed" not in item
            and "." not in item
        ):
            parse_recursive(of, folder_path + item + "/")
        # If the item is a file with a specific extension
        elif ".txt" in item or ".htm" in item:
            parse(of, folder_path, item)


if __name__ == "__main__":
    # User defined years to parse
    years = ["2017"]

    for year in years:
        parse_recursive(
            "//lazy_price/Company_Filings/" + year + "/",
            "//lazy_price/Company_Filings/" + year + "/",
        )
    # Note output is local not to the network drive
