document.addEventListener('DOMContentLoaded', function() {
    console.log('Participants interface loaded');

    const overlay = document.getElementById('loadingOverlay');
    if (overlay) {
        document.querySelectorAll('form').forEach(form => {
            form.addEventListener('submit', function() {
                overlay.style.display = 'flex';
            });
        });
    }
});
