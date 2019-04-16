$(document).ready(function(){
	var group = 0;
	var min = 1;
	var max = 3;
	var group = Math.floor(Math.random() * (+max - +min)) + +min;
	console.log(group);

	if (group == 1) {
		// Then experimental group
		document.getElementById("join_link").innerHTML = "emission://change_client?new_client=greentrip&clear_local_storage=true&clear_usercache=true";
		$("#customize").attr("src", "../img/join_study.png");
	} else {
		document.getElementById("join_link").innerHTML = "emission://change_client?new_client=urap2017control&clear_local_storage=true&clear_usercache=true";
		$("#customize").attr("src", "../img/join_study_control.png");
	};

});