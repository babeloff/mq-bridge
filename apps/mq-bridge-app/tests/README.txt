Create a showcase video:
npm install
npx playwright install chromium
SHOWCASE=true npx playwright test tests/playwright/showcase.spec.js --headed

# Convert the resulting video (path may vary slightly depending on your config)
ffmpeg -i test-results/showcase-Generate-showcase-video/video.webm \
       -vf "fps=15,scale=1280:-1:flags=lanczos,split[s0][s1];[s0]palettegen[p];[s1][p]paletteuse" \
       showcase.gif

ffmpeg -i test-results/showcase-Generate-showcase-video/video.webm \
  -vf "fps=4,split[s0][s1];[s0]palettegen=stats_mode=diff[p];[s1][p]paletteuse=dither=bayer:bayer_scale=5:diff_mode=rectangle" \
  -loop 0 \
  docs/images/Screen2.gif

Run as normal test (fast):
npx playwright test tests/playwright/showcase.spec.js
