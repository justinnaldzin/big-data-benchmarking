var data = source.data;
var column_names = source.column_names;  // ordering is random; to specify column ordering, define the array:
//var column_names = ['category', 'concurrency_factor', 'database', 'name', 'query_executed', 'query_id', 'query_template', 'rows', 'table_name', 'table_row_count', 'table_size_category', 'time'];
var filetext = column_names.join().concat('\n');
for (i=0; i < data['name'].length; i++) {
    var row = [];
    for (var c in column_names) {
      row = row.concat(['"' + data[column_names[c]][i].toString().replace(/"/g, '""') + '"']);
    }
    filetext = filetext.concat(row.join().concat('\n'));
}

var filename = 'big_data_benchmarking_results.csv';
var blob = new Blob([filetext], { type: 'text/csv;charset=utf-8;' });

//addresses IE
if (navigator.msSaveBlob) {
    navigator.msSaveBlob(blob, filename);
}

else {
    var link = document.createElement("a");
    link = document.createElement('a')
    link.href = URL.createObjectURL(blob);
    link.download = filename
    link.target = "_blank";
    link.style.visibility = 'hidden';
    link.dispatchEvent(new MouseEvent('click'))
}