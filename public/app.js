/*
 *
 *  * Copyright 2016 HM Revenue & Customs
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

var contextPath = '';
var pollerIsOk = true;

function setupContextPath() {
    var path = window.location.pathname.split( '/' );
    if (path[1] != 'ui') {
        contextPath = '/' + path[1];
    }
    console.log('contextPath='+contextPath);
}

function showStatus(url, cb) {
    $.ajax({
        url: contextPath + url,
        success: function (data) {
            pollerIsOk = true;
            $("#status").text(data[0].description);
            var queue = $("#queue");
            if (data.length > 1) {
                console.log(data);
                var s = "<ul>";
                for (var i = 1; i < data.length; i++) {
                    s += '<li><a href="#" onclick="cancelQueue(' + data[i].id + ')">X</a> ' + data[i].description + '</li>';
                }
                queue.html(s + '</ul>');
                queue.removeClass('empty');
            } else {
                queue.text("empty").addClass('empty');
            }
            if (cb) {
                cb();
            }
        },
        error: function(x, s, e) {
            pollerIsOk = false;
        }
    });
}

function refreshStatusContinually() {
    showStatus('/admin/viewQueue', function() {
        setTimeout(refreshStatusContinually, 1000);
    });
}

function cancelQueue(id) {
    showStatus('/admin/cancel/' + id);
}

function consoleText(text) {
    $("#console").removeClass('error').text(text);
}

function consoleError(text) {
    $("#console").addClass('error').text(text);
}

function ajax(method, path, success) {
    consoleText('Pending...');
    $.ajax({
        method: method,
        url: contextPath + path,
        success: success,
        error: function (xhr, status, error) {
            console.log(status);
            console.log(error);
            consoleError(status + '\n' + error);
        }
    });
}

function get(path, refresh, scrollToBottom) {
    ajax('GET', path, function (data) {
        consoleText(data);
        if (refresh)
            setTimeout(fullStatus, 250);
        if (scrollToBottom)
            $("html, body").animate({ scrollTop: $(document).height() }, "slow");
    });
}

function post(path, refresh) {
    ajax('POST', path, function (data) {
        consoleText(data);
        if (refresh)
            setTimeout(fullStatus, 250);
    });
}

function getAndRefreshConsoleJson(path) {
    ajax('GET', path, function (data) {
        consoleText(JSON.stringify(data, null, 2));
    });
}

function fullStatus() {
    get('/admin/fullStatus', false, true);
    if (!pollerIsOk) refreshStatusContinually();
}

//-------------------------------------------------------------------------------------------------

function checkbox(id, key) {
    var option = '';
    if ($('#'+id).is(":checked")) option = '&' + key + '=true';
    return option;
}

function doStartProductAction(action) {
    var product = $('#product').val();
    var epoch = $('#epoch').val();
    var variant = $('#variant').val();
    var bulkSize = '?bulkSize=' + $('#bulkSize').val();
    var loopDelay = '&loopDelay=' + $('#loopDelay').val();
    var streets = '&streetFilter=' + $('#streets').val();
    var include = '&include=' + $('#include').val();
    var prefer = '&prefer=' + $('#prefer').val();
    var force = checkbox('force', 'forceChange');
    if (product == '' || epoch == '' || variant == '')
        alert("Enter the product, epoch and variant");
    else
        get(action + '/' + product + '/' + epoch + '/' +
            variant + bulkSize + loopDelay + force +
            include + prefer + streets, true, true);
}

function getTarget1() {
    return $('#target1').val();
}

function doGo() {
    var target = getTarget1();
    doStartProductAction('/go/via/file/to/' + target);
}

function doFetch() {
    doStartProductAction('/fetch/to/file');
}

function doIngest() {
    var target = getTarget1();
    doStartProductAction('/ingest/from/file/to/' + target);
}

//-------------------------------------------------------------------------------------------------

function goAuto() {
    var target = getTarget1();
    get('/goAuto/via/file/to/' + target, true, false);
}

function cancelTask() {
    get('/admin/cancelCurrent', true, false);
}

function remoteTree() {
    get('/fetch/showRemoteTree', false, false);
}

function cleanFs() {
    post('/fetch/clean', true);
}

function cleanCol() {
    post('/indexes/es/clean', true);
}

function listCol() {
    getAndRefreshConsoleJson('/indexes/es/list');
}

function ping() {
    getAndRefreshConsoleJson('/ping');
}

function switchCol() {
    var colname = $('#colname').val();
    if (colname == '')
        alert("Enter the index name");
    else {
        colname = colname.replace(/_/g, '/');
        get('/switch/es/' + colname, true, false);
    }
}

function dropCol() {
    var colname = $('#colname').val();
    if (colname == '')
        alert("Enter the index name");
    else {
        ajax('DELETE', '/indexes/es/' + colname, function (data) {
            setTimeout(listCol, 250);
        });
    }
}

function dirTree() {
    var dir = $('#dirRoot').val();
    if (dir != '')
        dir = "root=" + dir + "&";
    var max = $('#dirMax').val();
    if (max != '')
        max = "max=" + max;
    ajax('GET', '/admin/dirTree?' + dir + max, function (data) {
        consoleText(data);
    });
}

function showLog() {
    var dir = $('#logDir').val();
    if (dir != '')
        dir = "dir=" + dir;
    ajax('GET', '/admin/showLog?' + dir, function (data) {
        consoleText(data);
    });
}

function toggleFieldset(id) {
    $(id + ' div').toggle();
    $(id).toggleClass('visible');
}

function showTheRealm() {
    ajax('GET', '/admin/realm', function (data) {
        var lines = data.split("\n");
        var realm = lines[0];
        $('#realm').addClass(realm).text(realm);
        var start = lines[1];
        $('#since').text("Up since " + start);
    });
}

//-------------------------------------------------------------------------------------------------

$(document).ready(
    function () {
        setupContextPath();
        showTheRealm();
        // buttons
        $('#fullStatusButton').click(fullStatus);
        $('#go').click(doGo);
        $('#fetch').click(doFetch);
        $('#ingest').click(doIngest);
        $('#goAuto').click(goAuto);
        $('#cancelTask').click(cancelTask);
        $('#cleanFs').click(cleanFs);
        $('#cleanCol').click(cleanCol);
        $('#listCol').click(listCol);
        $('#switchCol').click(switchCol);
        $('#dropCol').click(dropCol);
        $('#remoteTree').click(remoteTree);
        $('#ping').click(ping);
        $('#dirTree').click(dirTree);
        $('#showLog').click(showLog);
        // view toggles
        $('#fileActions legend').click(function() { toggleFieldset('#fileActions')} );
        $('#indexActions legend').click(function() { toggleFieldset('#indexActions')});
        $('#infoActions legend').click(function() { toggleFieldset('#infoActions')});
        getAndRefreshConsoleJson('/ping');
        refreshStatusContinually();
    }
);
