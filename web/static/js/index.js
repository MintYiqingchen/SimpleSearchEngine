$(document).ready(function () {
    // let test = JSON.parse('[{"docid":0, "score":1, "url":"a"},{"docid":1, "score":2, "url":"c"},{"docid":3, "score":2, "url":"d"},{"docid":4, "score":2, "url":"v"},{"docid":5, "score":6, "url":"c"},{"docid":6, "score":2, "url":"c"}]');
    // $.each(test, function (index, item) {
    //     console.log(item.docid);
    // });
    // let formDiv = $("#result");
    // let newTable = $(document.createElement("table"));
    // newTable.addClass("table table-striped table-hover");
    // let head = $(document.createElement("thead"));
    // head.addClass("table-light")
    // let title = $(document.createElement("tr"));
    // let th1 = $(document.createElement("th")).text("docid");
    // let th2 = $(document.createElement("th")).text("score");
    // let th3 = $(document.createElement("th")).text("url");
    // th1.attr("scope", "col");
    // th2.attr("scope", "col");
    // th3.attr("scope", "col");
    // title.append(th1);
    // title.append(th2);
    // title.append(th3);
    // head.append(title);
    // newTable.append(head);
    // let body = $(document.createElement("tbody"));
    // $.each(test, function (index, item) {
    //     let line = $(document.createElement("tr"));
    //     line.append($(document.createElement("td")).text(item.docid));
    //     line.append($(document.createElement("td")).text(item.score));
    //     line.append($(document.createElement("td")).text(item.url));
    //     body.append(line);
    // });
    // newTable.append(body);
    // formDiv.append(newTable);

    $("#search").click(function () {
        $("#search").prop("disabled", true);
        $("#result").empty();
        $.ajax({
            type: "POST",
            url: "/api/search",
            data: JSON.stringify({ query: $("#input").val() }),
            contentType: "application/json; charset=utf-8",
            dataType: "json",
        }).done(function (data) {
            let formDiv = $("#result");
            let newTable = $(document.createElement("table"));
            newTable.addClass("table table-striped table-hover table-bordered");
            let head = $(document.createElement("thead"));
            head.addClass("table-light")
            let title = $(document.createElement("tr"));
            let th1 = $(document.createElement("th")).text("DocId");
            let th2 = $(document.createElement("th")).text("Score");
            let th3 = $(document.createElement("th")).text("URL");
            th1.attr("scope", "col");
            th2.attr("scope", "col");
            th3.attr("scope", "col");
            title.append(th1);
            title.append(th2);
            title.append(th3);
            head.append(title);
            newTable.append(head);
            let body = $(document.createElement("tbody"));
            $.each(data, function (index, item) {
                let line = $(document.createElement("tr"));
                line.append($(document.createElement("td")).text(item.docid));
                line.append($(document.createElement("td")).text(item.score));
                let a = $(document.createElement("a"));
                a.attr("href", item.url).attr("target", "_blank").text(item.url);
                line.append($(document.createElement("td")).append(a));
                body.append(line);
            });
            newTable.append(body);
            formDiv.append(newTable);
        }).always(function () {
            $("#search").prop("disabled", false);
        });
    });
});