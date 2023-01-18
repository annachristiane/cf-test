def delete_at_index(list, index):
    try:
        list.pop(index)
    except IndexError as error:
        print("Index doesn't exsit or empty list.")
