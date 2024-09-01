from flask import Flask, render_template, request, make_response
import pandas as pd
import numpy as np
import functools
import json
from datetime import datetime
import os
import mysql.connector

application = Flask(__name__)


def get_sql_query(dict_selected_values = {}):
    where = ""
    columns = ["rain","temp","wetb","dewpt","vappr","rhum","msl","wdsp","wddir","sun","vis","clht","clamt"]
    if dict_selected_values != {}:
        where = "WHERE "
        for key in dict_selected_values.keys():
            if key != "row":
                if key == "drop":
                    for val in dict_selected_values[key]:
                        columns.remove(val)
                elif key not in ("month", "year"):
                    where += f"{key} IN ("
                    for val in dict_selected_values[key]:
                        where += f"'{val}',"
                    where = where.rstrip(",")
                    where += ") AND "
                elif key == "month":
                    where += f"month(date) IN ("
                    for val in dict_selected_values[key]:
                        where += f"'{val}',"
                    where = where.rstrip(",")
                    where += ") AND "
                elif key == "year":
                    where += f"year(date) IN ("
                    for val in dict_selected_values[key]:
                        where += f"'{val}',"
                    where = where.rstrip(",")
                    where += ") AND "
        where = where.rstrip(" AND ")

    return "SELECT county,station,convert(date, CHAR),"+ ",".join(col for col in columns) +" FROM weather_data " + where


def get_data(dict_selected_values = {}):
   try:
        try:
            if 'RDS_HOSTNAME' in os.environ:
                    DATABASE = {
                            'ENGINE': 'django.db.backends.mysql',
                            'NAME': os.environ['RDS_DB_NAME'],
                            'USER': os.environ['RDS_USERNAME'],
                            'PASSWORD': os.environ['RDS_PASSWORD'],
                            'HOST': os.environ['RDS_HOSTNAME'],
                            'PORT': os.environ['RDS_PORT']
                    }

            mydb = mysql.connector.connect(
            host=DATABASE['HOST'],
            user=DATABASE['USER'],
            password=DATABASE['PASSWORD'],
            database=DATABASE['NAME']
            )

        except: # if database connection fails, read from csv
            data = pd.read_csv("data.csv", sep=',')
            data["date"] = pd.to_datetime(data["date"])
            if dict_selected_values != {}:
                temp = []
                for key in dict_selected_values.keys():
                    if key == "drop":
                        data = data.drop(columns=dict_selected_values[key])
                    elif key not in ("month", "year"):
                        if key in data.columns:
                            temp += [data[key].astype(str).isin(dict_selected_values[key])]
                    elif key == "month":
                        temp += [data["date"].dt.strftime("%b").isin(dict_selected_values[key])]
                    elif key == "year":
                        temp += [data["date"].dt.strftime("%Y").isin(dict_selected_values[key])]
                
                if temp != []:
                    data = data.loc[functools.reduce(np.logical_and, temp)]
            return data

        data = pd.read_sql(get_sql_query(dict_selected_values), mydb)
        data["convert(date, CHAR)"] = pd.to_datetime(data[data.columns[2]].str[:-3])
        data.rename(columns={"convert(date, CHAR)": "date"}, inplace=True)
        
        data[data == 0] = np.nan

        mydb.close()
            
        return data
   except Exception as e:
        return str(e) + "<br>"


@application.route('/add', methods=['POST'])
def add():
    try:
        selected_values = request.get_json()
        try:
            if 'RDS_HOSTNAME' in os.environ:
                DATABASE = {
                        'ENGINE': 'django.db.backends.mysql',
                        'NAME': os.environ['RDS_DB_NAME'],
                        'USER': os.environ['RDS_USERNAME'],
                        'PASSWORD': os.environ['RDS_PASSWORD'],
                        'HOST': os.environ['RDS_HOSTNAME'],
                        'PORT': os.environ['RDS_PORT']
                }

            mydb = mysql.connector.connect(
            host=DATABASE['HOST'],
            user=DATABASE['USER'],
            password=DATABASE['PASSWORD'],
            database=DATABASE['NAME']
            )

        except: # if database connection fails, read from csv
            data = pd.read_csv("data.csv", sep=',')
            data["date"] = pd.to_datetime(data["date"])
            for key in selected_values:
                if key == "date":
                    selected_values[key] = datetime.strptime(selected_values[key], '%Y-%m')
            
            data.loc[-1] = selected_values.values()
            data.to_csv("data.csv", index=False)
            return "OK"

        mycursor = mydb.cursor()
        for key in selected_values:
            if key == "date":
                selected_values[key] = f"STR_TO_DATE('{str(selected_values[key])}', '%Y-%m')"
            else:
                selected_values[key] = f"'{str(selected_values[key])}'"

        mycursor.execute("INSERT INTO weather_data VALUES (" + str(",".join(str(val) for val in selected_values.values())) + ")")

        mycursor.close()
        mydb.commit()
        mydb.close()
        return "OK"
    except Exception as e:
        return str(e) + "<br>"

@application.route('/delete', methods=['POST'])
def delete():
    try:
        if 'RDS_HOSTNAME' in os.environ:
            DATABASE = {
                    'ENGINE': 'django.db.backends.mysql',
                    'NAME': os.environ['RDS_DB_NAME'],
                    'USER': os.environ['RDS_USERNAME'],
                    'PASSWORD': os.environ['RDS_PASSWORD'],
                    'HOST': os.environ['RDS_HOSTNAME'],
                    'PORT': os.environ['RDS_PORT']
            }
        
        mydb = mysql.connector.connect(
            host=DATABASE['HOST'],
            user=DATABASE['USER'],
            password=DATABASE['PASSWORD'],
            database=DATABASE['NAME']
        )
    except: # if database connection fails, use csv
        data = pd.read_csv("data.csv", sep=',')
        data["date"] = pd.to_datetime(data["date"])
        
        for row in request.get_json():
            data = data.loc[(data["county"] != row[0]) & (data["station"] != row[1]) & (data["date"] != row[2])]
        data.to_csv("data.csv", index=False)
        return "OK"
    
    mycursor = mydb.cursor()
    for row in request.get_json():
        mycursor.execute(f"DELETE FROM weather_data WHERE county='{row[0]}' AND station='{row[1]}' AND date=STR_TO_DATE('{row[2]}', '%m/%Y')")
    mycursor.close()
    mydb.commit()
    mydb.close()
    return "OK"


@application.route('/', methods=['GET'])
def main():
   try:
        message = '<head><script src="https://cdn.jsdelivr.net/npm/chart.js"></script></head><body style="background-color:#fff1c4">'
        selected_values = request.args.to_dict(flat=False)
        
        columns = ["county", "station", "date", "rain", "temp", "wetb", "dewpt", "vappr", "rhum", "msl", "wdsp", "wddir", "sun", "vis", "clht", "clamt"]
        
        data = get_data(selected_values)
        if isinstance(data, str): # If error occurs, return the error
            return data
        
        # input new data
        message += '''<div><b>Enter new data:</b><br>'''
        message += '<form id="add-form">'
        for col in columns:
            message += f'<div style="display: inline-block">'
            message += f'<label for="input-{col}">{col}</label><br>'
            message += f'<input type={"text" if data[col].dtype == "object" else "number step=any" if col != "date" else "month"} ' \
                    f'id="input-{col}" list="input-options-{col}" style="width:60px" {"required" if col in ["county", "station", "date"] else ""}>'
            if col in data.select_dtypes(["object"]):
                message += f'<datalist id="input-options-{col}">'
                for elem in sorted(data[col].unique()):
                    message += f'<option value="{elem}" />'
                message += f'</datalist>'
            message += '</div>'
        message += '<br><input type="button" value="Enter" onclick="insert_row()">'
        message += ' <span id="error-insert" style="display: none; color: red">Error: Please fill in all fields</span>'
        message += '</form>'
        message += '<div id="loading" style="position:absolute; padding: 40px; font-size: 30px;background-color: white; bottom:50%; left: calc(50% - 150px); border: 2px solid black; display: none">LOADING...</div>'

        # javascript for inserting new data
        message += '''<script>
                      function insert_row() {
                        var form = document.getElementById("add-form");
                        var errorInsert = document.getElementById("error-insert");
                        var isValid = form.checkValidity();
                        if (isValid) {
                            fetch('/add', {
                                method: 'POST',
                                headers: {
                                'Accept': 'application/json',
                                'Content-Type': 'application/json'
                                },
                                body: JSON.stringify({''' + ', '.join(f'{col}: document.getElementById("input-{col}").value' for col in columns) + '''})
                            }).then(function(response) {
                                if (response.ok) {
                                    var loading = document.getElementById("loading");
                                    loading.style.display = "block";
                                    location.reload();
                                } else {
                                    errorInsert.innerHTML = "Error: Cannot insert row";
                                    errorInsert.style.display = "block";
                                }
                            });
                        } else {
                            errorInsert.innerHTML = "Error: The values are not valid";
                            errorInsert.style.display = "block";
                        }
                      }
                      </script>'''


        # javascript for selecting and deleting rows
        message += '''<script>
                        var selectedRows = [];
                        function changeSelectedRows(index) {
                            if (selectedRows.includes(index)) {
                                selectedRows = selectedRows.filter(item => item !== index);
                            } else {
                                selectedRows.push(index);
                            }
                        };'''

        message += '''function delete_selected() {
                        var valuesToDelete = [];
                        for (var i = 0; i < selectedRows.length; i++) {
                            valuesToDelete.push([]);
                            for (var j = 1; j < document.getElementById("data-table").rows[selectedRows[i]].cells.length; j++) {
                                valuesToDelete[i].push(document.getElementById("data-table").rows[selectedRows[i]].cells[j].innerText.trim());
                            }
                        }

                        fetch('/delete', {
                            method: 'POST',
                            headers: {
                                'Accept': 'application/json',
                                'Content-Type': 'application/json'
                            },
                            body: JSON.stringify(valuesToDelete)
                        }).then(function(response) {
                            if (response.ok) {
                                var loading = document.getElementById("loading");
                                loading.style.display = "block";
                                location.reload();
                            } else {
                                document.getElementById("delete-error").style.display = "block";
                            }
                        });
                    }
                    </script>'''

        message += '<div style="box-sizing:border-box;display:inline-block; width:48%"><form method="get" action="/">'
        # add table
        message += '<div style="width:100%; border:3px solid orange;height:700px; overflow:scroll">'
        message += '<table id="data-table" style="border: 1px solid black; border-collapse: collapse; width:500px">'
        message += '<tr>'
        message += '<th style="border: 1px solid black; box-sizing:border-box;"></th>'
        
        # header
        for col in columns:
            message += f'<th style="border: 1px solid black; padding: 5px; box-sizing:border-box; position: relative;">{col}'
            # dropdown button
            message += f'<br><button id="{col}-button" type="button">&#9660;</button>&nbsp;'
            # dropdown
            message += f'<div id="{col}-dropdown" class="dropdown-content" style="display: none; position: absolute; top: 100%; left: 0; width: 100%; height: 300px; overflow-y:scroll; border: 1px solid black; background-color:white; text-align: left;">'
            if col == "date":
                message += '<span>Year</span><br>'
                for val in data[col].dt.strftime('%Y').unique():
                    message += f'<input id="check-year-{val}" type="checkbox" name="year" value="{val}"><span>{val}</span><br>'
                message += '<span>Month</span><br>'
                for val in data[col].dt.strftime('%b').unique():
                    message += f'<input id="check-month-{val}" type="checkbox" name="month" value="{val}"><span>{val}</span><br>'
            else:
                for val in sorted(data[col].unique(), reverse=True):
                    message += f'<input id="check-{col}-{val}" type="checkbox" name="{col}" value="{val}"><span>{val}</span><br>'
            message += '</div></th>'
        
        message += '</tr>'

        # add rows in the table
        for index, row in data.sort_values(by="date", ignore_index=True).iterrows():
            message += '<tr>'
            message += f'<td style="border: 1px solid black; box-sizing:border-box;"><input type="checkbox" id="check-row-{index}" onclick="changeSelectedRows({index+1})"></td>'
            
            # values in each row
            for col in columns:
                if col == "date":
                    message += f'<td style="border: 1px solid black; padding: 5px; box-sizing:border-box;">{row[col].strftime("%m/%Y")}&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>'
                else:
                    message += f'<td style="border: 1px solid black; padding: 5px; box-sizing:border-box;">{str(row[col])}&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>'
            
            message += '</tr>'
        message += '</table></div><br>'

        # delete button
        message += '<input type="button" value="Delete Selected Rows" onclick="delete_selected()" style="display:block;margin:0px"><span id="delete-error" style="display: none; color: red">Error deleting the rows</span><br>'
        # filter and reset button
        message += '<input id="filter" type="submit" value="FILTER" style="padding: 5px;font-weight:bold; font-size: 2rem;">'
        message += '</form><form>'
        message += '<input id="reset" type="submit" value="RESET" style="padding: 5px;font-weight:bold; font-size: 2rem;">'
        message += '</form></div>'

        # ------------------------------
        # javascript for filter, reset and dropdown
        message += '<script>'
        for key, values in selected_values.items():
            for val in values:
                message += f'document.getElementById("check-{key}-{val}").checked = true;'
        message += 'document.addEventListener("DOMContentLoaded", function() {'
        message += '  var filterButton = document.getElementById("filter");'
        message += '  var resetButton = document.getElementById("reset");'
        message += '  var loading = document.getElementById("loading");'
        message += '  filterButton.addEventListener("click", function(e) {loading.style.display = "block"});'
        message += '  resetButton.addEventListener("click", function(e) {loading.style.display = "block"});'

        for col in columns:
            message += f'  var {col}Button = document.getElementById("{col}-button");'
            message += f'  var {col}Dropdown = document.getElementById("{col}-dropdown");'
            message += f'  {col}Button.addEventListener("click", function(e) {{ e.stopPropagation(); {col}Dropdown.style.display = ({col}Dropdown.style.display === "none") ? "block" : "none"; }});'
            message += f'  {col}Dropdown.addEventListener("click", function(e) {{ e.stopPropagation(); }});'
        message += '  document.addEventListener("click", function() {'
        for col in columns:
            message += f'    {col}Dropdown.style.display = "none";'
        message += '  });'
        message += '});'
        message += '</script>'

        # ---------------GRAPH---------------

        message += """
        <div style="box-sizing:border-box;padding: 15px;display:inline-block;width:50%;vertical-align:top;">
        <canvas id="myChartCanvas" width="800" height="400"></canvas>

        <div style="padding: 15px;display:inline-block;width:100%;vertical-align:top;">
        """
        # add buttons for stations
        for station in data["station"].unique():
                message += f"<button style='display: inline-block' onclick=\"update_graph('{station}')\">{station}</button>"
        message += "</div><div style='padding: 15px;display:inline-block;width:100%;vertical-align:top;'>"

        # add buttons for columns
        for col in data.columns:
            if col != "date" and col != "county" and col != "station":
                message += f"<input type='checkbox' {'checked=checked' if col == 'rain' else ''} style='display: inline-block' onclick=\"dropColumn('{col}')\">{col}"

        message += """</div>Click boxes to change columns in the graph, click filter to update filter<br>
                            <b>rain:</b> registered rain for the month<br>
                            <b>temp:</b> average temperature for the month (day and night)<br>
                            <b>wetb:</b> Wet bulb temperature<br>
                            <b>dewpt:</b> Dew point temperature<br>
                            <b>vappr:</b> Vapor pressure<br>
                            <b>rhum;</b> Relative humidity<br>
                            <b>msl:</b> Mean sea level pressure<br>
                            <b>wdsp:</b> Wind speed<br>
                            <b>wddir:</b> Wind direction<br>
                            <b>sun:</b> Sunshine duration<br>
                            <b>vis:</b> Visibility<br>
                            <b>clht:</b> Cloud height<br>
                            <b>clamt:</b> Cloud amount</div>
        <script>
        var myChart;
        var selected_station = '"""+ data["station"].unique()[0] +"""';
        var columnDropped = ["temp","wetb","dewpt","vappr","rhum","msl","wdsp","wddir","sun","vis","clht","clamt"];

        function dropColumn(column) {
            if (columnDropped.includes(column)) {
                columnDropped = columnDropped.filter(item => item !== column);
            } else {
                columnDropped.push(column);
            }
            update_graph();
        }

        function get_drop_query() {
            if (columnDropped.length === 0) {
                return "";
            }
            return "&" + columnDropped.map(item => `drop=${item}`).join("&");
        }

        function update_graph(station) {
            if (station != undefined) {
                selected_station = station;
            }
            fetchData(`station=${selected_station}${get_drop_query()}&"""+'&'.join(['&'.join(key + '=' + value for value in values) for key, values in selected_values.items() if key != 'station'])+"""`);
        }

        function fetchData(query) {
            fetch(`/get_data?${query}`)
                .then(response => response.json())
                .then(data => {
                    updateChart(data);
                });
        }

        function updateChart(data) {
            if (myChart) {
                myChart.destroy();
            }

            var ctx = document.getElementById("myChartCanvas").getContext("2d");
            myChart = new Chart(ctx, {
                type: "line",
                data: {
                    labels: data.date,
                    datasets: []
                },
                options: {
                    plugins: {
                        title: {
                            display: true,
                            text: selected_station,
                            font: {
                                size: 30
                            }
                        }
                    },
                    scales: {
                        y: {
                        beginAtZero: true
                        }
                    }
                }
            });

            var columnColors = {
                "rain": "rgb(31, 119, 180)", 
                "temp": "rgb(255, 127, 14)",
                "wetb": "rgb(44, 160, 44)",
                "dewpt": "rgb(214, 39, 40)", //red
                "vappr": "rgb(148, 103, 189)",
                "rhum": "rgb(140, 86, 75)",
                "msl": "rgb(227, 119, 194)",
                "wdsp": "rgb(127, 127, 127)",
                "wddir": "rgb(188, 189, 34)",
                "sun": "rgb(23, 190, 207)",
                "vis": "rgb(31, 184, 42)",
                "clht": "rgb(228, 26, 28)",
                "clamt": "rgb(55, 126, 184)"
            }

            for (var col in data) {
                if (col !== "date") {
                    myChart.data.datasets.push({
                        label: col,
                        data: data[col],
                        backgroundColor: columnColors[col],
                        borderColor: columnColors[col],
                        borderWidth: 1
                    });
                }
            }
            myChart.update();
        }
        """
        # fetch data when first loading the page
        if selected_values != {}:
            message += f"fetchData('station={data['station'].unique()[0]}'+get_drop_query()+'&{'&'.join('&'.join(key + '=' + value for value in values) for key, values in selected_values.items() if key != 'station')}')</script>"
        else:
            message += f"fetchData('station={data['station'].unique()[0]}'+get_drop_query())</script>"
        message += '</body>'

        return message
   except Exception as e:
         return message + str(e) + "<br>"

@application.route('/get_data')
def get_data_page():
    selected_values = request.args.to_dict(flat=False)

    data = get_data(selected_values)
    if isinstance(data, str):
        return data
    result = {
        'date': data['date'].dt.strftime('%b %Y').tolist()
    }

    for col in data.columns:
        if col not in ("date", "county", "station"):
            result[col] = data[col].tolist()

    # avoid returning html formated page
    response = make_response(json.dumps(result).replace("NaN", "null"), 200)
    response.mimetype = "text/plain"

    return response


@application.route('/del_db')
def delete_sql_database():
    message = ""
    try:
        if 'RDS_HOSTNAME' in os.environ:
            DATABASE = {
                    'ENGINE': 'django.db.backends.mysql',
                    'NAME': os.environ['RDS_DB_NAME'],
                    'USER': os.environ['RDS_USERNAME'],
                    'PASSWORD': os.environ['RDS_PASSWORD'],
                    'HOST': os.environ['RDS_HOSTNAME'],
                    'PORT': os.environ['RDS_PORT']
            }
        
        mydb = mysql.connector.connect(
            host=DATABASE['HOST'],
            user=DATABASE['USER'],
            password=DATABASE['PASSWORD'],
            database=DATABASE['NAME']
        )

        mycursor = mydb.cursor()
        mycursor.execute("DROP TABLE IF EXISTS weather_data")
        mycursor.close()
        mydb.commit()
        mydb.close()
        message += "Database deleted"
    except Exception as e:
        message += str(e) + "<br>"
    
    return message


@application.route('/gen_db')
def generate_sql_database():
    message = ""
    try: 
        if 'RDS_HOSTNAME' in os.environ:
            DATABASE = {
                    'ENGINE': 'django.db.backends.mysql',
                    'NAME': os.environ['RDS_DB_NAME'],
                    'USER': os.environ['RDS_USERNAME'],
                    'PASSWORD': os.environ['RDS_PASSWORD'],
                    'HOST': os.environ['RDS_HOSTNAME'],
                    'PORT': os.environ['RDS_PORT']
            }

        mydb = mysql.connector.connect(
        host=DATABASE['HOST'],
        user=DATABASE['USER'],
        password=DATABASE['PASSWORD'],
        database=DATABASE['NAME']
        )

        mycursor = mydb.cursor()
        mycursor.execute("DROP TABLE IF EXISTS weather_data")
        mycursor.execute("""CREATE TABLE weather_data (
                            county VARCHAR(255),
                            station VARCHAR(255),
                            date DATE,
                            rain FLOAT,
                            temp FLOAT,
                            wetb FLOAT,
                            dewpt FLOAT,
                            vappr FLOAT,
                            rhum FLOAT,
                            msl FLOAT,
                            wdsp FLOAT,
                            wddir FLOAT,
                            sun FLOAT,
                            vis FLOAT,
                            clht FLOAT,
                            clamt FLOAT,
                            PRIMARY KEY (county, station, date)
                            );""")

        data = pd.read_csv("data.csv", sep=",")

        # Insert data into database
        for row in data.iterrows():
            for key in row[1].keys():
                if key == "date":
                    row[1][key] = f"STR_TO_DATE('{str(row[1]['date'])}', '%Y-%m')"
                else:
                    row[1][key] = f"'{str(row[1][key])}'"

            mycursor.execute("INSERT INTO weather_data VALUES (" + str(",".join(str(val) for val in row[1])) + ")")

        # print data if all correct
        mycursor.execute("SELECT county,station, convert(date, CHAR),rain,temp,wetb,dewpt,vappr,rhum,msl,wdsp,wddir,sun,vis,clht,clamt FROM weather_data")

        results = mycursor.fetchall()
        mycursor.close()
        mydb.commit()
        mydb.close()

        for row in results:
            message += str(row) + "<br>"
    except Exception as e:
        message += str(e) + "<br>"

    return message

if __name__ == '__main__':
    application.run(debug=True)
