/**
 * Accessibility scan of each main view.
 *
 * The toolbars are mostly icon-only controls, which is exactly where an
 * accessible name goes missing without anyone noticing: the button still looks
 * right, and only a screen reader (or this scan) can tell it now announces
 * itself as "button". Serious and critical violations fail; the lighter ones are
 * attached to the report so they can be worked through without blocking a run.
 */
const { test, expect } = require("./fixtures");
const AxeBuilder = require("@axe-core/playwright").default;
const { resetConfig, gotoView } = require("./helpers");

const VIEWS = [
  { name: "publishers", hash: "#publishers:0" },
  { name: "consumers", hash: "#consumers:0" },
  { name: "config", hash: "#config" },
];

const BLOCKING_IMPACTS = ["serious", "critical"];

/**
 * Violations that exist today. They are bugs, not exemptions: the list is here so
 * the scan can run in CI and fail on anything *new* while these are worked
 * through. Delete an entry as soon as its rule is fixed — a listed rule that no
 * longer fires fails this suite too, which is what keeps the list honest.
 *
 *  - color-contrast: the muted greys in the sidebars, proto badges and the
 *    danger button. Fixing it is a palette decision, not a local edit.
 *  - nested-interactive: `wa-button` is given `role="button"` and `tabindex="0"`
 *    while rendering its own button in its shadow root. Removing the redundant
 *    pair touches every panel and the keyboard paths the sweep covers.
 *  - label / select-name: the request bar's protocol, method and URL controls
 *    are labelled by adjacent spans that are not associated with them.
 *  - aria-input-field-name: the CodeMirror editors expose a textbox with no name.
 */
const KNOWN_VIOLATIONS = {
  publishers: [
    "aria-input-field-name",
    "color-contrast",
    "label",
    "nested-interactive",
    "select-name",
  ],
  consumers: ["aria-input-field-name", "color-contrast", "label", "nested-interactive"],
  config: ["color-contrast"],
};

function summarize(violations) {
  return violations
    .map(
      (violation) =>
        `${violation.impact}: ${violation.id} — ${violation.help} (${violation.nodes.length} node(s))\n` +
        violation.nodes
          .slice(0, 5)
          .map((node) => `      ${node.target.join(" ")}`)
          .join("\n"),
    )
    .join("\n  ");
}

for (const view of VIEWS) {
  test(`${view.name} view has no serious accessibility violations`, async ({ page }, testInfo) => {
    await resetConfig(page);
    await gotoView(page, view.hash);

    const { violations } = await new AxeBuilder({ page })
      .withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"])
      .analyze();

    const blocking = violations.filter((violation) => BLOCKING_IMPACTS.includes(violation.impact));
    const advisory = violations.filter((violation) => !BLOCKING_IMPACTS.includes(violation.impact));

    if (advisory.length > 0) {
      await testInfo.attach(`a11y-advisory-${view.name}`, {
        body: summarize(advisory),
        contentType: "text/plain",
      });
    }

    const known = KNOWN_VIOLATIONS[view.name] ?? [];
    const introduced = blocking.filter((violation) => !known.includes(violation.id));
    expect(
      introduced,
      `new accessibility violations in ${view.name}:\n  ${summarize(introduced)}`,
    ).toEqual([]);

    await testInfo.attach(`a11y-known-${view.name}`, {
      body: summarize(blocking.filter((violation) => known.includes(violation.id))),
      contentType: "text/plain",
    });

    const fixed = known.filter((id) => !blocking.some((violation) => violation.id === id));
    expect(
      fixed,
      `these no longer fail — delete them from KNOWN_VIOLATIONS.${view.name} in a11y.spec.js`,
    ).toEqual([]);
  });
}
