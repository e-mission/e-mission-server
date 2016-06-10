'use strict';

module.exports = function(grunt){
    // Load grunt tasks automatically
    require('load-grunt-tasks')(grunt);

    //Project Settings
    var options = {
        config: {
            src: 'grunt/*.js',
            dist: 'dist',
            example: 'example',
            app: 'app'
        }
    };

    // Load grunt configurations automatically
    var configs = require('load-grunt-configs')(grunt, options);

    // Define the configuration for all the tasks
    grunt.initConfig(configs);

    grunt.registerTask('buildExample', [
        'cssmin:example',
        'uglify:example'
    ]);

    grunt.registerTask('buildDist', [
        'cssmin:dist',
        'uglify:dist'
    ]);

    grunt.registerTask('serve', [
        'buildExample',
        'connect',
        'watch'
    ]);

    grunt.registerTask('default', [
        'serve'
    ]);
};