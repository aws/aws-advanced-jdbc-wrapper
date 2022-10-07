$(document).ready(function(){
    
    //[Anclas con animación - Global]
    $('a[href*="#"]:not([href="#"]):not([href="#skip"])').click(function(){
        if (location.pathname.replace(/^\//,'') == this.pathname.replace(/^\//,'') && location.hostname == this.hostname) {
            var target = $(this.hash);
            target = target.length ? target : $('[name=' + this.hash.slice(1) +']');

            if (target.length && window.innerWidth > 900) {
                $('html, body').animate({
                    scrollTop: (target.offset().top - 120)
                }, 1000);

                return false;
            } else {
                $('html, body').animate({
                    scrollTop: (target.offset().top - 100)
                }, 1000);

                return false;
            }
        }
    });
    
    //[Menu]
    if ( window.innerWidth > 900 ) {
        $('.mdl-header label').click(function(){
            $('body').toggleClass('over-hidden');
        });
    }
    
    $('.mdl-header nav a').click(function(){
        $('.mdl-header label').click();
    });

    //[Sticky Header]
    $(window).on('scroll', function() {
        var scrollPos = $(document).scrollTop();
        console.log(scrollPos);

        if (window.innerWidth > 900) {
            if (scrollPos > 500){
                $('.sticky-header').slideDown(250, 'linear');
            } else {
                $('.sticky-header').slideUp(250, 'linear');
            }
        } else if (window.innerWidth < 900) {
            if (scrollPos > 450) {
                $('.sticky-header').slideDown(250, 'linear');
            } else {
                $('.sticky-header').slideUp(250, 'linear');
            }
        }
    });

    //[Scroll to Top]
    $('.s-logo').click(function(){
        $('html, body').animate({ scrollTop: 0 }, 1000);
        return false;
    });
    
});

$(window).load(function(){
    $('.sta-masonry').each(function(){
        $(this).masonry();
    });
});

