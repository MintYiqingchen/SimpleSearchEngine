$(document).ready(function () {
    // let test = JSON.parse('[{"docid":0, "score":1, "url":"a"},{"docid":1, "score":2, "url":"c"},{"docid":3, "score":2, "url":"d"},{"docid":4, "score":2, "url":"v"},{"docid":5, "score":6, "url":"c"},{"docid":6, "score":2, "url":"c"}]');
    // $.each(test, function (index, item) {
    //     console.log(item.docid);
    // });
    // let formDiv = $("#result");
    // let newTable = $(document.createElement("table"));
    // let title = $(document.createElement("tr"));
    // title.append($(document.createElement("td")).text("docid"));
    // title.append($(document.createElement("td")).text("score"));
    // title.append($(document.createElement("td")).text("url"));
    // newTable.append(title);
    // $.each(test, function (index, item) {
    //     let line = $(document.createElement("tr"));
    //     line.append($(document.createElement("td")).text(item.docid));
    //     line.append($(document.createElement("td")).text(item.score));
    //     line.append($(document.createElement("td")).text(item.url));
    //     newTable.append(line);
    // });
    // formDiv.append(newTable);

    $("#search").click(function () {
        $("#search").prop("disabled", true);
        let word = $("#input").val();
        console.log(word);
        $.ajax({
            type: "POST",
            url: "/api/search",
            data: JSON.stringify({ query: $("#input").val() }),
            contentType: "application/json; charset=utf-8",
            dataType: "json",
        }).done(function (data) {
            let formDiv = $("#result");
            let newTable = $(document.createElement("table"));
            let title = $(document.createElement("tr"));
            title.append($(document.createElement("td")).text("docid"));
            title.append($(document.createElement("td")).text("score"));
            title.append($(document.createElement("td")).text("url"));
            newTable.append(title);
            $.each(data, function (index, item) {
                let line = $(document.createElement("tr"));
                line.append($(document.createElement("td")).text(item.docid));
                line.append($(document.createElement("td")).text(item.score));
                line.append($(document.createElement("td")).text(item.url));
                newTable.append(line);
            });
            formDiv.append(newTable);
        }).always(function () {
            $("#search").prop("disabled", false);
        });
    });
});