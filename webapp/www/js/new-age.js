(function($) {
  "use strict"; // Start of use strict

  // Smooth scrolling using jQuery easing
  $('a.js-scroll-trigger[href*="#"]:not([href="#"])').click(function() {
    if (location.pathname.replace(/^\//, '') == this.pathname.replace(/^\//, '') && location.hostname == this.hostname) {
      var target = $(this.hash);
      target = target.length ? target : $('[name=' + this.hash.slice(1) + ']');
      if (target.length) {
        $('html, body').animate({
          scrollTop: (target.offset().top - 48)
        }, 1000, "easeInOutExpo");
        return false;
      }
    }
  });

/*
  $('a.badge-link').click(function() {
    alert("The apps are currently under development. Please contact the CEO for an invite to the beta");
  });
*/

  // Closes responsive menu when a scroll trigger link is clicked
  $('.js-scroll-trigger').click(function() {
    $('.navbar-collapse').collapse('hide');
  });

  // Activate scrollspy to add active class to navbar items on scroll
  $('body').scrollspy({
    target: '#mainNav',
    offset: 54
  });

  // Collapse Navbar
  var navbarCollapse = function() {
    if ($("#mainNav").offset().top > 100) {
      $("#mainNav").addClass("navbar-shrink");
    } else {
      $("#mainNav").removeClass("navbar-shrink");
    }
  };
  // Collapse now if page is not at top
  navbarCollapse();
  // Collapse the navbar when page is scrolled
  $(window).scroll(navbarCollapse);

})(jQuery); // End of use strict

window.onload = init;
function init(){
  /*
  var link = document.getElementById("groupLink");
  var image = document.getElementById("phone");
  var QRC = qrcodegen.QrCode;
  var qr0 = QRC.encodeText("Hello, world!", QRC.Ecc.MEDIUM);
  var canvas = document.getElementById("qrcode-canvas");
  canvas.style.display = "none";
  var myrandom = Math.floor(Math.random() * 3) + 1;
  if (myrandom == 1) {
    link.href = "emission://change_client?new_client=urap2017information&clear_local_storage=true&clear_usercache=true"
    image.src = "img/information.jpg"
  }
  else if (myrandom == 2) {
    link.href = "emission://change_client?new_client=urap2017emotion&clear_local_storage=true&clear_usercache=true"
    image.src = "img/emotion.png"
  } else {
    link.href = "emission://change_client?new_client=urap2017control&clear_local_storage=true&clear_usercache=true"
    image.src = "img/control.jpg"
  }
  qr0 = QRC.encodeText(link.href, QRC.Ecc.MEDIUM);
  qr0.drawCanvas(4, 1, canvas);
  canvas.style.removeProperty("display");
  */
}
