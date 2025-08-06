/**
 * It creates a new list item, adds it to the list, and increments the hidden input that keeps track of
 * the number of list items.
 * @param thing - the UL element that is the parent of the LI elements that are to be sorted
 */
function addSortable(thing) {
	//alert('Add code to add list item\n' + thing);
	var prefix = thing.id.substring(0, thing.id.indexOf("ScraperList"));

	var cntobj = document.getElementById(thing.id + "Count");
	cntobj.value = parseInt(cntobj.value) + 1;
	var suffix = parseInt(cntobj.value) + 1;
	var newitem = document.createElement("li");
	newitem.classList.add("list-group-item");
	newitem.classList.add("keyvalpair");
	var obj = document.createElement("i");
	obj.classList.add("drag-handle");
	obj.classList.add("fas");
	obj.classList.add("fa-bars");
	newitem.appendChild(obj);   // creates the dragable section in the field

	var obj = document.createElement("sub");
	obj.innerHTML = suffix;	// the hidden INPUT is already incrementing this by one, but we're making one extra for unsortable KeyField
	newitem.appendChild(obj);
	newitem.appendChild( document.createTextNode(" ") );
	var obj = document.createElement("input");
	obj.type = "hidden";
	obj.name=prefix+"sort_"+suffix;
	obj.id=prefix+"sort_"+suffix;
	obj.value=suffix;
	newitem.appendChild(obj);

	var obj = document.createElement("input");
	obj.type = "text";
	obj.name=prefix+"nom_"+suffix;
	obj.id=prefix+"nom_"+suffix;
	obj.value="";
	obj.classList.add("nom");
	newitem.appendChild(obj);

	newitem.appendChild( document.createTextNode("─") );
	var obj = document.createElement("input");
	obj.type = "text";
	obj.name=prefix+"val_"+suffix;
	obj.id=prefix+"val_"+suffix;
	obj.setAttribute("list", prefix+"list");
	obj.value="";
	obj.classList.add("val");
	newitem.appendChild(obj);

	var obj = document.createElement("i");
	obj.classList.add("status");
	obj.classList.add("far");
	obj.classList.add("fa-circle");
	newitem.appendChild(obj);
	thing.appendChild(newitem);
}

function renumber(draggablelistobject) {
	j = 2;
	for (var i = 0; i < draggablelistobject.children.length; i++) {
		var obj = draggablelistobject.children[i];
		if (obj.nodeName == "LI") {
			var item = obj.getElementsByTagName("sub")[0];
			item.innerHTML = j;
			var item = obj.getElementsByTagName("input")[0];
			if (item.type == "hidden") {
				item.value = j;
			}
			j++;
		}
	}
}

function removeScraperItem(thing){
	var lastItem = thing.lastElementChild;

	while(thing.firstChild) {
		thing.removeChild(thing.firstChild);
	}

}

function removeSingleScraperItem(button){
	var item = button.parentNode;
	var indexOfItem = Array.prototype.indexOf.call(item.parentNode.childNodes,item);
	item.parentNode.removeChild(item)	;
	// console.log("removed item");
}

function sortByAlphabeticallIndex(fields){

	var items = fields.querySelectorAll('input');
	var itemsArray = Array.from(items)

	itemsArray.sort(function(a,b){
		var aValue = parseInt(a.value);
		var bValue = parseInt(b.value);
		if (aValue < bValue) {
		  return -1;
		}
		if (aValue > bValue) {
		  return 1;
		}
		return 0;
	});

	for (var i = 0; i < itemsArray.length; i++) {
		list.appendChild(itemsArray[i].parentNode);
	}
}
