> [!IMPORTANT]
> This document contains mandatory **human-facing guidelines** for LLM use in this repository.
> It is not intended as _input_ for LLMs.

# Contributing Guidelines with respect to LLMs

## Intro

We are a passionate but small team of contributors. We believe that Large Language Models (LLMs) offer a powerful tool to enhance our efficiency and accelerate conservation impact.  At its best, we can add virtual AI team members at very low cost.

This can refer to anything from the ChatGPT web chat portal, to a chat-driven IDE integration like Cursor, to a fully automated AI Agent like Jules or Codex.

This document sets guidelines for the appropriate use (and non-use) of LLMs in our organization's open-source projects.

### Philosophy

The guidance in the rest of this document aims to uphold two core tenets:

**Software developers should FEEL OWNERSHIP over the codebase.**

* There is value in being able to speak knowledgably about what is easy/hard, possible/impossible in meetings and other scenarios away from LLMs.
* It’s important to be able to quickly diagnose and solve bugs, outages, or performance problems in production-facing deployments where sensitive environments sits, making it more challenging to interact with LLMs.

**Honor human-to-human collaboration.**

Your teammates chose to work with you, not with your AI assistant. Honor that choice by ensuring your communications reflect your genuine understanding and perspective.

* Delivering correct code and clear prose demonstrates competence.
* Advancing discussions in meetings and conversations with your own insights and questions shows respect for the collaborative process.
* The risk of poor AI output belongs with the AI user.  Never allow it to waste your collaborators' time.

## Master your tools, find what works for you

This document does not intend to micromanage. This document does not specify a workflow or process to develop software.  Definitely find a workflow that works for you. This may include not using LLMs at all.

There are dozens of good blogs and tutorials highlighting what people have found to be best practices for their own personal workflow.  Check them out and see what works.  The answer will probably change as technology evolves.

However those documents do not address working in a team: that’s what this doc is for.

## Same expectations for LLM-produced content as for human-produced content

Ultimately we keep the same expectations we’ve had for years, from [the contributing guidelines for “meatspace” contributors](./CONTRIBUTING.md): code linters and formatters settle code style debates, post focused and cohesive pull requests, etc.  Interpreting these in the context of LLMs:

* Code comments should share the “why”, not restate the line of code (LLMs love to parrot every line of code they write in another comment)
* Don't erase existing code comments unless directly relevant to your change (LLMs love to erase unrelated comments).
* For documentation and dialogue, LLMs tend to be verbose and rephrase content across different sections in a highly redundant manner, or include implementation details that are better off being included in the body of a PR. Please carefully review documentation to cull excessive details before submitting.
* Keep scope in-check: limit PRs to the goal at hand. No extra code beyond what is absolutely necessary to solve the problem the user provides (LLMs love to fix unrelated technical debt).

Whether a human or LLM authored it, all merged code is subject to the same expectations we’d have of a trusted human contributor.

## Be the human in the loop

Prose (documentation, design docs, PR descriptions, emails, and dialogue in chat, code review or on GitHub Issues) should be written *for an audience of humans*, not for LLMs.  Remember that even though you might lean heavily on an LLM, your colleague might not.

* Some LLMs tend to spew repetitive or overly verbose text. Try to reduce the burden of review on others by eliminating superfluous, irrelevant, or repetitive information in your text.  (Your human colleague is not going to ask their LLM to summarize the 2-page emoji-filled PR description that yours output.  Give them something they can read themself.)
* Whereas LLMs can be flattering and paint everything in a positive light and with great certainty, written communication in our team needs to expose shortcomings, questions, gray areas, and possible failure cases.

### Preserve Human Dialogue

Remote collaboration already faces significant challenges, so high-fidelity communication matters. When discussing with teammates, clients, or external partners through email, chat, code reviews, or GitHub Issues, always engage directly in a genuine, human-to-human dialogue. While LLMs are powerful tools for refining your thinking or checking grammar, the final communication should represent *your* personal understanding and voice.

* When responding to colleagues, try to preserve their original language rather than paraphrasing because their word choices may be carefully chosen to carry precise meaning.  LLMs habitually rephrase content, which risks creating confusion about whether the restated version is agreement or disagreement.
* Collaboration tax: Adding more participants to communication increases overhead, requiring more effort to make sure everyone is aligned. LLM output effectively adds an invisible “participant”  that teammates must decode and check if they are aligned with.

Email, chat, code review & on GitHub Issues are all forums where people come expecting to interact with other humans. Don’t force them to interact with your LLM.

### Design Docs

Design Docs, in particular, are dense with hard questions and proposed answers that will have broad and long-lasting implications. Grant Slatton writes in “[How to write a good design document](https://grantslatton.com/how-to-design-document)”,

> The goal of a design document is to convince the reader the design is optimal given the situation.
>
> The most important person to convince is *the author*. The act of writing a design document helps to add rigor to what are otherwise vague intuitions. Writing reveals how sloppy your thinking was (and later, code will show how sloppy your writing was).

When constructing a design doc, feel free to use LLMs as a technical sounding board, but it is exceptionally important that the final choices are yours and that you communicate your level of confidence to your colleagues.

## Oversight and Verification

### LLM as collaborative tool, not authority

“Treat the model/system/agent as a *junior but competent pair programming colleague,* while also realizing that LLMs are autoregressive next token generators” ([source](https://newsletter.victordibia.com/p/developers-stop-asking-llms-genai)).  All outputs are a result of hallucination, which means they can be either correct or incorrect, and the model has no mechanism to differentiate between the two.

* Refine an LLM’s first output to improve it with your expertise as a software engineer, computer scientist, and/or subject matter expert.
* LLM output should prompt analysis, never replace your expert judgement.

### Never Trust, Always Verify

Review *and verify* LLM output thoroughly.

* **The first reviewer of the code or documentation that you submit should be YOU\!**  (More at [https://blog.beanbaginc.com/2014/12/01/practicing-effective-self-review/](https://blog.beanbaginc.com/2014/12/01/practicing-effective-self-review/))
* NEVER represent an LLM’s claims as your own unless you’ve validated or independently know they are true: this is misrepresentation and can erode trust.
* All code, documentation, or suggestions generated by LLMs must be *reviewed and tested*. Never merge or publish AI-generated code without human approval and security scans.  As stated in [the contributing guidelines](https://github.com/ConservationMetrics/gc-scripts-hub/blob/main/CONTRIBUTING.md), the responsibility for bug-free code remains on the code author,  not the code reviewer or product manager.

**Test-driven development becomes more important** than ever: With more code being written faster and faster, the onus switches to how to verify that it’s correct.

* Tests should be correct & rigorous, and of course should pass.
* LLMs are prone to “reward hacking,” including when a model cheats in order to get tests to pass—for example hard-coding or special-casing a value. Or, generating trivial tests that don’t actually meaningfully validate the feature under scrutiny.
  * As a human expert, you absolutely must review and understand test cases inside and out\!
  * It can also be a good idea to try to break the function and verify the test case fails.

### Know your Limits

When you are out of your league (e.g. just vibe-coded a quadcopter flight controller in Lisp), then you probably don’t even recognize your own blind spots and cannot realistically validate LLM output to the extent desired. And the burden on a knowledgeable reviewer will be excessively high.  In this case you should couch the output only as a prototype or an idea for discussion, not as production-ready.  Any exceptions should be discussed as a team and handled on a case-by-case basis.

### AI use must be disclosed for code contributions (PRs)

If you are using any kind of AI assistance to contribute code, disclose it in the pull request. Our PR templates will include a section for this. As a small exception, trivial tab-completion doesn't need to be disclosed, so long as it is limited to single keywords or short phrases.  ([inspired by ghostty](https://github.com/ghostty-org/ghostty/pull/8289))

The purpose of disclosure is not to judge but to reinforce accountability. By disclosing you acknowledge your role in supervising and vetting the AI's output before asking for review.

## Reviewing code is a powerful way to feel ownership

Reviewing each others’ code is a remarkably effective way to learn the codebase and therefore come to *feel ownership*.  It’s also the predominant way most people practice reading code, a necessary skill for reviewing code generated by your own LLMs, as previously advised in this document. Therefore we *strongly encourage you* to review PRs as a human (both your own code and collaborators’).

LLM reviewers can be helpful in a couple ways:

1. LLMs can help find your bearings in a particularly large or confusing PR (or to help the code author write a better PR description in the first place\!), so you can review more effectively.
2. We are open to adding an AI bot to conduct automated code review on PRs, but we would treat this as a second opinion, and does not replace a human review.

## Choose the right tool for the job

Don’t use LLMs for things that other tools already do better:

* linting/formatting
* running unit tests
* automated scanning tools (such as [Static Application Security Testing (SAST)](https://docs.gitlab.com/user/application_security/sast/), [Software Composition Analysis (SCA)](https://docs.gitlab.com/user/application_security/dependency_scanning/), and [Secret Detection](https://docs.gitlab.com/user/application_security/secret_detection/))

AI Bots excel at certain management & overhead tasks.  We strongly encourage exploring opportunities for this.  Examples from [Managing your repo with AI — What works, and why open-source will win](https://www.youtube.com/watch?v=xIOh_7_wTnw) include:

* [Dosu Support bot](https://dosu.dev/) to curate GitHub issues (add labels, detect duplicates). Since LLMs are a collaborative tool, not replacement, humans should still close their own issues.
* Auto-generate release notes
* Use Slack chatbots to answer questions from the community.  HOWEVER keep bots contained to their own Channel, as to not misrepresent an LLM’s overly confident response as from a human.
* I18N: Use LLMs to automatically propose translations (for human expert review).

## Working with Sensitive Data

This section’s guidance focuses on third-party LLMs. Locally hosted LLMs are exempt, as long as data remains within organizational boundaries.

### Partner/Client data

Partner and client trust depends on knowing their sensitive information stays within agreed boundaries. LLMs represent an additional third party in that chain of custody.

When working with data that belongs to, was created by, or concerns our partners and clients, apply stricter limitations on LLM use:

* Never input partner/client data into LLMs without explicit consent. This includes data produced by or about partners and communications with partners.
* Default to human-only analysis when reviewing, debugging, or discussing partner data or deployments.

### Security and Secrets Management

It’s always a good idea to keep secrets out of your prompts, but in practice we don’t think this guidance is very enforceable or auditable by the user of some AI Agents. A better solution is to prevent mistakes in the first place by not issuing secrets that must not be leaked.

* **Immediate:** Use environment variables, credential management services, and LLM-specific ignore files to keep secrets out of AI agents and out of code and logs where they might be accidentally copied into LLM prompts.
* **Ongoing:** We’re working to reduce team members’ direct access to production deployments and implement tighter scoping and rotation of credentials to limit damage from any accidental disclosure.

### Transcription and Translation of meetings

Make sure that the service you use has a "no training" clause, to not train or fine-tune their models without customer’s prior permission; and make sure you haven’t given that permission.

[Google Workspace, including Meet, offers this.](https://services.google.com/fh/files/misc/genai_privacy_google_cloud_202308.pdf) (Also, local transcription works, if not very effective).

## Out of Scope (for now)

The following are things that we intentionally leave out-of-scope for now.  Either we think there are better ways to handle these issues than “Contributing Guidelines” or the problem is still not well enough understood.

1. **Choice of model or tool:** Choose the specific provider, model, or tooling based on what works best for your workflow. Of the cloud-hosted services, we suspect all are equally bad in terms of security and data sovereignty.  A consequence is that we are not yet checking-in “agent instructions” to Git repos (e.g. [AGENT.md](https://ampcode.com/AGENT.md), [CLAUDE.md](https://www.anthropic.com/engineering/claude-code-best-practices#a-create-files)), since at the moment these tend to be too specific to choice of tool.
2. **User-facing AI tools:** We are thinking about how to approach the possibility of integrating user-facing AI into the software we deliver.  That’s a big topic that warrants further exploration. We will address this more comprehensively and amend this document as we develop our understanding.
3. **Energy consumption:** We acknowledge the significant energy consumption associated with LLMs, which presents a tension with our commitment to accelerating conservation impact.  We're still figuring out the best way to address this conflict and ensure we make responsible choices.
4. **Bias:** Understanding and mitigating bias in LLMs is a crucial area that requires further research. Given our small team and the rapidly changing models, conducting this research isn't feasible for us right now. We’ll keep an eye on developments as we refine these guidelines.

## References for further reading

NOTE: There are dozens of articles focused more on getting better output from an LLM (prompt engineering, vibe-coding workflows, etc). We omit those here because they are likely to change and also more personal than the concerns at the team level that this document aims to address.

The following are relevant to *responsible use* of LLMs or to the use of LLMs o*n distributed/collaborative projects.*

* CMI Guardian Connector [Contributing Guidelines](https://github.com/ConservationMetrics/gc-scripts-hub/blob/main/CONTRIBUTING.md)
* [**Best Practices for Large Language Models**](https://guides.library.cmu.edu/LLM_best_practices)
  1. Collaborative tool, not replacement
  2. Verify Critical Information
  3. Inspiration and Ideation
  4. Use Specific Instructions
  5. Terminology and Concept Lookups
  6. Code Understanding
  7. Test and Validate Generated Code
  8. Recognize Subject Limitations
  9. Adopt Appropriate Skepticism
  10. Balance Automation with Skill Development
  11. Explore Multiple Perspectives
  12. Present Ideas as Someone Else's
  13. Structure Complex Learning Projects
* [LLMs and Agents as Team Enablers](https://www.infoq.com/news/2024/08/llm-agent-team-enablers/)
  1. How AI agents can act as “team members” in Scrum and onboarding
  2. LLM-generated output risks overwhelming teams with noise
  3. Lessons from real-world trials of autonomous coding agents
* [Managing your repo with AI — What works, and why open-source will win](https://www.youtube.com/watch?v=xIOh_7_wTnw) : Maintaining an OSS repository is hard. Scaling contributors is nigh impossible. As AI platforms proliferate, let’s take a look at tools you should and shouldn’t leverage in automating your GitHub repo, Slack workspace, and more\! We’ll also talk about why Open Source Software stands to benefit more from this revolution than private/proprietary codebases.
  1. Dosu bot to help with Github Issues and automatic release notes.
  2. Keep Slack chatbots contained to their own channel.
  3. They don’t do automated PR review. Want to encourage the feeling of membership in a community of people.
  4. Human Oversight
  5. Agentic AI workflows (e.g. pushing to consensus, augmentation, closing the loop)
* [3 best practices for building software in the era of LLMs](https://about.gitlab.com/blog/3-best-practices-for-building-software-in-the-era-of-llms/) (new security habits)
  1. Never trust, always verify: Never merge or publish AI-generated code without human approval and security scans
  2. Prompt for secure patterns: Specifically ask LLMs to follow secure coding practices, and reference team security guidelines in your prompts.
  3. Scan everything, no exceptions: Run all LLM-generated code through security tools and manual validation before inclusion in your codebase.
* [How to Effectively Use Generative AI for Software Engineering Tasks\!](https://newsletter.victordibia.com/p/developers-stop-asking-llms-genai)
  1. Don’t write code \- Analyze First\!
  2. Focus on Providing Context
  3. Ask Many Questions. Learn
  4. Watch out for Subtle Mistakes
  5. Aim for knowledge parity.
  6. Invest in Good Design
  7. Where do agents fit into all this?
