import os
import sys
import csv
import pickle
import pandas as pd

class CSVConverter:
    def __init__(self, collection=None, filter=None):
        self.trait_file_folder = 'trait_files/'
        self.txt_file_folder = 'txt_files/'
        self.pickle_file_folder = 'pickle_files/'
        if not os.path.isdir(self.trait_file_folder):
            os.mkdir(self.trait_file_folder)
        if not os.path.isdir(self.txt_file_folder):
            os.mkdir(self.txt_file_folder)
        self.column_index_converter = {}
        for num in range(1, 100):
            letter = chr((num % 26)+ 96)
            if letter == '`':
                letter = 'z'
            letter = letter.upper()
            if num % 26 != num and num % 26 > 0:
                prefix = chr(int(num / 26) + 96).upper()
            else:
                prefix = ''
            self.column_index_converter[num] = prefix + letter
        self.AllNFTs = {}
        self.AllTraits = {}
        self.collection = collection
        if self.collection == 'Hisportory':
            self.traitConverter = {'No.': 'Number', \
                                   'Pos': 'Position', \
                                   'G': 'Games'}
        self.filter = filter


    def convertCSVgroup(self, group_name):
        if group_name == 'Detroit Lions 1938':
            self.CSV_dict = {'Detroit Lions 1938 Lifetime Stats.csv': '', \
                             'Detroit Lions 1938 Scoring Summary.csv': '', \
                             'Detroit Lions 1938 Kicking and Punting Stats.csv': 'Kicking & Punting ', \
                             'Detroit Lions 1938 Passing Stats.csv': 'Passing ', \
                             'Detroit Lions 1938 Rushing and Receiving Stats.csv': 'Rushing & Receiving ', \
                             'Detroit Lions 1938 Defensive and Fumble Stats.csv': 'Defensive & Fumble '}
            self.redundant_traits = ['No.', 'Player', 'Age', 'Pos', 'G', 'GS', 'Tm', 'Year']
            
        for CSV_name in self.CSV_dict:
            self.convertCSVtoTXT(CSV_name)
        for NFT_name in self.AllNFTs:
            pickle.dump(self.AllNFTs[NFT_name], open(self.pickle_file_folder + NFT_name + '.pickle', 'wb'))
        pickle.dump(self.AllTraits, open(self.pickle_file_folder + 'AllTraits.pickle', 'wb'))
        new_csv_file = open('Cumulative CSV.csv', 'w')
        column_names = list(self.AllTraits)
        csv_writer = csv.DictWriter(new_csv_file, column_names)
        column_names_dict = {}
        for name in column_names:
            column_names_dict[name] = name
        csv_writer.writerow(column_names_dict)
        for row_dict in self.AllNFTs:
            csv_writer.writerow(self.AllNFTs[row_dict])

    def convertListOfDictsToCSV(self, list_of_dicts, column_names, file_name = 'new_csv'):
        dataframe = pd.DataFrame(list_of_dicts, columns=column_names)
        print(column_names)
        dataframe.to_csv(file_name + '.csv')
        return(dataframe)

    def convertBytesToCSV(self, bytes_object, file_name='new_csv'):
        big_str = ''
        for line in bytes_object:
            big_str += str(line).split("b'")[1]
        big_str_list = big_str.split('\\n')
        big_str = ''
        for chunk in big_str_list:
            big_str += chunk
            big_str += ','
        split_str = big_str.split(',')
        column_names = [split_str[0]]
        for entry in split_str:
            if "'" in entry:
                entry = entry.split("'")[0] + entry.split("'")[1]
            if ('                        ') in entry:
                column_names.append(entry.split('                        ')[1])
        for num in range(len(column_names)):
            del split_str[0]
        list_of_dicts = []
        entry_count = 0
        for entry in split_str:
            if "'" in entry:
                entry = entry.split("'")[0] + entry.split("'")[1]
            if '\n' in entry:
                entry = entry.split("\\n")[1]
            if entry_count % len(column_names) == 0:
                if entry_count > 0:
                    list_of_dicts.append(row_dict)
                row_dict = {}
            row_dict[column_names[entry_count % len(column_names)]] = entry
            entry_count += 1
        return(self.convertListOfDictsToCSV(list_of_dicts, column_names, file_name))
            

    def convertListOfListsToCSV(self, list_of_lists, column_names, file_name='new_csv'):
        list_of_dicts = []
        accumulating_row_list = []
        for row_list in list_of_lists:
            for entry in row_list:
                accumulating_row_list.append(entry)
                if len(accumulating_row_list) == len(column_names):
                    row_dict = {}
                    index = 0
                    for name in column_names:
                        row_dict[name] = accumulating_row_list[index]
                        index += 1
                    list_of_dicts.append(row_dict)
                    accumulating_row_list = []
        return(self.convertListOfDictsToCSV(list_of_dicts, column_names, file_name))


    def convertJSONtoCSV(self, json):
        list_of_dicts = []
        for row_dict in json:
            new_row_dict = {}
            for key in row_dict:
                value = row_dict[key]
                next_entry = {key: value}
                while type(next_entry[key]) == list or type(next_entry[key]) == dict:
                    if type(next_entry[key]) == list:
                        if type(next_entry[key][0]) == list or type(next_entry[key][0]) == dict:
                            next_entry = {key: next_entry[key][0]}
                            continue
                        else:
                            combined_list = ''
                            for list_entry in next_entry[key]:
                                combined_list += list_entry + '; '
                            next_entry = {key: combined_list}
                    elif type(next_entry[key]) == dict:
                        combined_dict = {}
                        for dict_entry in next_entry[key]:
                            combined_dict.update({dict_entry: next_entry[key][dict_entry]})
                        next_entry = combined_dict
                        break
                new_row_dict.update(next_entry)
            list_of_dicts.append(new_row_dict)
        CSV_file = self.convertListToCSV(list_of_dicts, list(new_row_dict))
        return(CSV_file)
                        
        

    def convertCSVtoTXT(self, file_name=None):
        self.input_file_name = file_name
        print(self.input_file_name, self.CSV_dict[self.input_file_name])
        csv_reader = csv.DictReader(open(file_name, 'r'))
        row_count = 0
        for row_dict in csv_reader:
            row_count += 1
            self.createTraitFile(row_dict, row_count)
        print(self.AllNFTs)

    def filterCSVgroup(self, file_names=[]):
        list_of_CSVs = []
        for file_name in file_names:
            list_of_CSVs.append(self.filterCSV(file_name, write_csv=False))
        final_csv_list = []
        for row_dict in list_of_CSVs[0]:
            for row_dict_B in list_of_CSVs[1]:
                if row_dict['Email'] == row_dict_B['Email']:
                    row_dict_B.update(row_dict)
                    column_names = row_dict_B
                    final_csv_list.append(row_dict_B)
        column_names_dict = {}
        for name in column_names:
            column_names_dict[name] = name
        new_csv_file = open('Combined and Filtered CSV.csv', 'w')
        csv_writer = csv.DictWriter(new_csv_file, column_names)
        csv_writer.writerow(column_names_dict)
        for row_dict in final_csv_list:
            csv_writer.writerow(row_dict)
            
        

    def filterCSV(self, file_name=None, write_csv=True):
        csv_reader = csv.DictReader(open(file_name, 'r'))
        new_csv_list = []
        new_csv_file = open('Filtered CSV.csv', 'w')
        for row_dict in csv_reader:
            column_names = list(row_dict)
        csv_reader = csv.DictReader(open(file_name, 'r'))
        column_names.append('Weird Email Address')
        column_names.append('Weird Twitter ID')
        csv_writer = csv.DictWriter(new_csv_file, column_names)
        column_names_dict = {}
        for name in column_names:
            column_names_dict[name] = name
        csv_writer.writerow(column_names_dict)
        if self.filter == 'MetaNoise':
            count = 0
            for row_dict in csv_reader:
                count += 1
                row_dict['Weird Email Address'] = 'False'
                row_dict['Weird Twitter ID'] = 'False'
                write_row = True
                if count <= 3300:
                    print(count)
                    for key in row_dict:
                        if row_dict[key] != '':
                            if key == 'Order Date':
                                order_date = row_dict[key]
                                order_date_no_symbols = order_date.split('-')[0] + order_date.split('-')[1] + order_date.split('-')[2]
                                order_date_no_symbols = order_date_no_symbols.split(':')[0] + order_date_no_symbols.split(':')[1] + order_date_no_symbols.split(':')[2]
                                order_date_no_symbols = order_date_no_symbols.split(' ')[0] + order_date_no_symbols.split(' ')[1]
                                if not(order_date_no_symbols.isalnum()):
                                    print(order_date_no_symbols)
                                    print(key, row_dict[key], row_dict[key].isalnum())
                                    write_row = False
                            # Premint Data
                            elif key == 'date':
                                order_date = row_dict[key]
                                order_date_no_symbols = order_date.split('-')[0] + order_date.split('-')[1] + order_date.split('-')[2]
                                order_date_no_symbols = order_date_no_symbols.split(':')[0] + order_date_no_symbols.split(':')[1] + order_date_no_symbols.split(':')[2] + order_date_no_symbols.split(':')[3]
                                order_date_no_symbols = order_date_no_symbols.split(' ')[0] + order_date_no_symbols.split(' ')[1]
                                order_date_no_symbols = order_date_no_symbols.split('+')[0] + order_date_no_symbols.split('+')[1]
                                order_date_no_symbols = order_date_no_symbols.split('.')[0] + order_date_no_symbols.split('.')[1]
                                if not(order_date_no_symbols.isalnum()):
                                    print(order_date_no_symbols)
                                    print(key, row_dict[key], row_dict[key].isalnum())
                                    write_row = False
                            # Premint Data
                            elif key == 'eth balance at registration':
                                eth_balance = row_dict[key]
                                if '.' in eth_balance:
                                    eth_balance_no_symbols = eth_balance.split('.')[0] + eth_balance.split('.')[1]
                                else:
                                    eth_balance_no_symbols = eth_balance
                                if 'E' in eth_balance_no_symbols:
                                    eth_balance_no_symbols = eth_balance_no_symbols.split('E-')[0] + eth_balance_no_symbols.split('E-')[1]
                                else:
                                    eth_balance_no_symbols = eth_balance_no_symbols
                                if not(eth_balance_no_symbols.isalnum()):
                                    print(eth_balance_no_symbols)
                                    print(key, row_dict[key], row_dict[key].isalnum())
                                    write_row = False
                            # Premint Data
                            elif key == 'twitter account created':
                                account_created = row_dict[key]
                                account_created_no_symbols = account_created.split(':')[0] + account_created.split(':')[1] + account_created.split(':')[2]
                                account_created_no_symbols = account_created_no_symbols.split('+')[0] + account_created_no_symbols.split('+')[1]
                                account_created_no_symbols_list = account_created_no_symbols.split(' ')
                                account_created_no_symbols = ''
                                for word in account_created_no_symbols_list:
                                    account_created_no_symbols += word
                                if not(account_created_no_symbols.isalnum()):
                                    print(account_created_no_symbols)
                                    print(key, row_dict[key], row_dict[key].isalnum())
                                    write_row = False
                            # Premint Data
                            elif key == 'twitter username':
                                user_name = row_dict[key]
                                if '_' in user_name:
                                    user_name_no_symbols_list = user_name.split('_')
                                    user_name_no_symbols = ''
                                    for word in user_name_no_symbols_list:
                                        user_name_no_symbols += word
                                else:
                                    user_name_no_symbols = user_name
                                if not(user_name_no_symbols.isalnum()):
                                    print(user_name_no_symbols)
                                    print(key, row_dict[key], row_dict[key].isalnum())
                                    write_row = False
                            elif key == 'Email':
                                email_address = row_dict[key]
                                if '@' in email_address and '.' in email_address and email_address[0] != '@':
                                    email_address_no_symbols = email_address.split('@')[0] + email_address.split('@')[1]
                                    email_address_no_symbols = email_address_no_symbols.split('.')[0] + email_address_no_symbols.split('.')[1]
                                    if '_' in email_address_no_symbols:
                                        email_address_no_symbols_list = email_address_no_symbols.split('_')
                                        email_address_no_symbols = ''
                                        for word in email_address_no_symbols_list:
                                            email_address_no_symbols += word
                                    if '+' in email_address_no_symbols:
                                        row_dict['Weird Email Address'] = 'True'
                                        print('Weird email...')
                                        print(email_address)
                                    elif not(email_address_no_symbols.isalnum()):
                                        print(email_address_no_symbols)
                                        print(key, row_dict[key], row_dict[key].isalnum())
                                        #write_row = False
                                        row_dict['Weird Email Address'] = 'True'
                                        print('Weird email...')
                                else:
                                    row_dict['Weird Email Address'] = 'True'
                                    print('Weird email...')
                                    print(email_address)
                            elif key == 'Ticket Type':
                                ticket_type = row_dict[key]
                                if '/' in ticket_type and '&' in ticket_type:
                                    ticket_type_no_symbols = ticket_type.split('/')[0] + ticket_type.split('/')[1]
                                    ticket_type_no_symbols = ticket_type_no_symbols.split('&')[0] + ticket_type_no_symbols.split('&')[1]
                                    ticket_type_no_symbols_list = ticket_type_no_symbols.split(' ')
                                else:
                                    ticket_type_no_symbols_list = ticket_type.split(' ')
                                ticket_type_no_symbols = ''
                                for word in ticket_type_no_symbols_list:
                                    ticket_type_no_symbols += word
                                if not(ticket_type_no_symbols.isalnum()):
                                    print(ticket_type_no_symbols)
                                    print(key, row_dict[key], row_dict[key].isalnum())
                                    write_row = False
                            elif key == 'Order Type':
                                order_type = row_dict[key]
                                order_type_no_symbols_list = order_type.split(' ')
                                order_type_no_symbols = ''
                                for word in order_type_no_symbols_list:
                                    order_type_no_symbols += word
                                if not(order_type_no_symbols.isalnum()):
                                    print(order_type_no_symbols)
                                    print(key, row_dict[key], row_dict[key].isalnum())
                                    write_row = False
                            elif key == 'First Name':
                                first_name = row_dict[key]
                                first_name_no_symbols_list = first_name.split(' ')
                                first_name_no_symbols = ''
                                for word in first_name_no_symbols_list:
                                    first_name_no_symbols += word
                                if not(first_name_no_symbols.isalnum()):
                                    print(first_name_no_symbols)
                                    print(key, row_dict[key], row_dict[key].isalnum())
                                    write_row = False
                            elif key == 'Last Name':
                                last_name = row_dict[key]
                                last_name_no_symbols_list = last_name.split(' ')
                                last_name_no_symbols = ''
                                for word in last_name_no_symbols_list:
                                    last_name_no_symbols += word
                                if '-' in last_name_no_symbols:
                                    last_name_no_symbols = last_name_no_symbols.split('-')[0] + last_name_no_symbols.split('-')[1]
                                if not(last_name_no_symbols.isalnum()):
                                    print(last_name_no_symbols)
                                    print(key, row_dict[key], row_dict[key].isalnum())
                                    write_row = False
                            elif key == 'Total Paid' or key == 'Eventbrite Fees' or key == 'Eventbrite Payment Processing':
                                amount = row_dict[key]
                                amount_no_symbols = amount.split('.')[0]
                                if len(amount.split('.')) == 2:
                                    amount_no_symbols = amount.split('.')[1]
                                if not(amount_no_symbols.isalnum()):
                                    print(amount_no_symbols)
                                    print(key, row_dict[key], row_dict[key].isalnum())
                                    write_row = False
                            elif not(row_dict[key].isalnum()):
                                print(key, row_dict[key], row_dict[key].isalnum())
                                #write_row = False
                                print('Weird Twitter ID...')
                                row_dict['Weird Twitter ID'] = 'True'
                    if write_row:
                        new_csv_list.append(row_dict)
                    else:
                        print('ROW NOT WRITTEN!!!')
        if write_csv:
            for row_dict in new_csv_list:
                csv_writer.writerow(row_dict)
        else:
            return(new_csv_list)

    def createTraitFile(self, row_dict, row_count):
        trait_file_lines = self.initializeTraitFile()
        trait_dict = {}
        txt_file_lines = ''
        last_trait = False
        trait_count = 0
        for trait_type in row_dict:
            if trait_type == 'Player':
                self.player_name = row_dict[trait_type].split('*')[0]
        record_trait = False
        for trait_type in row_dict:
            trait_count += 1
            value = row_dict[trait_type]
          # Record traits in AllTraits dict
            if self.AllTraits.get(self.convertTraitType(trait_type)):
                self.AllTraits[self.convertTraitType(trait_type)].append(value)
            else:
                self.AllTraits[self.convertTraitType(trait_type)] = [value]
          # Record individual NFT traits
            if self.AllNFTs.get(self.player_name):
                if not(self.AllNFTs[self.player_name].get(trait_type)):
                    record_trait = True
            else:
                record_trait = True
            if record_trait:
                trait_type = self.convertTraitType(trait_type)
                trait_dict[trait_type] = value
                if len(row_dict) == trait_count:
                    last_trait = True
                trait_file_lines = self.addTraitToFile(trait_file_lines, trait_type, value, last_trait)
                txt_file_lines += trait_type + ': ' + value + '\n'
        if self.collection == 'Hisportory':
            self.output_file_name = self.player_name.split('*')[0]
        if self.AllNFTs.get(self.output_file_name):
            self.AllNFTs[self.output_file_name].update(trait_dict)
        else:
            self.AllNFTs[self.output_file_name] = trait_dict
        self.finalizeTraitFile(trait_file_lines, row_count)
        txt_file = open(self.txt_file_folder + self.output_file_name + '.txt', 'a')
        txt_file.write(txt_file_lines)
        txt_file.close()
        

    def finalizeTraitFile(self, trait_file_lines, image_number):
        trait_file = open(self.trait_file_folder + self.output_file_name + '.json', 'a')
        for line in trait_file_lines:
            trait_file = self.addLine(trait_file, line)
        trait_file.close()
    
    def initializeTraitFile(self, description='None', image='None', name='None'):
        trait_file_lines = []
##        trait_file_lines.append('{')
##        trait_file_lines.append('    "description": "' + description + '",')
##        trait_file_lines.append('    "image": "' + image + '",')
##        trait_file_lines.append('    "name": "' + name + '",')
##        trait_file_lines.append('    "attributes": [')
        return(trait_file_lines)

    def addTraitToFile(self, trait_file_lines, trait_type, value, last_trait=False):
        trait_file_lines.append('        {')
        trait_file_lines.append('              "trait_type": "' + trait_type + '",')
        trait_file_lines.append('              "value": "' + value + '"')
        if last_trait:
##            trait_file_lines.append('        }]}')
            trait_file_lines.append('        },')
        else:
            trait_file_lines.append('        },')
        return(trait_file_lines)

    def convertTraitType(self, trait_type):
      # Adds a prefix to the trait type to distinguish it from identcal trait types in other files         
        if not(trait_type in self.redundant_traits):
            trait_type = self.CSV_dict[self.input_file_name] + trait_type
        return(trait_type)
        

    def addLine(self, trait_file, line):
        trait_file.write(line + '\n')
        return(trait_file)


if __name__ == '__main__':
    #CSVC = CSVConverter(collection='Hisportory', filter='MetaNoise')
    #CSVC.convertCSVtoTXT('Detroit Lions 1938 Scoring Summary.csv')
    #CSVC.convertCSVgroup('Detroit Lions 1938')
    #CSVC.filterCSV('Premint Data v3.csv')
    #CSVC.filterCSVgroup(['Premint Data v3.csv', 'Eventbrite Data v1.csv'])
    shit = 'balls'
    

