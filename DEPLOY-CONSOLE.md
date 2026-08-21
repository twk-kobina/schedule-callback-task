# Deploying the Scheduled Callback System (AWS Console)

This guide walks through deploying `callback-system.yaml` entirely from the AWS
Management Console — no CLI required. The template creates two DynamoDB tables,
two Lambda functions (with placeholder code), two IAM roles, and the permissions
that let Amazon Connect invoke the functions. After the stack is up, you paste
the real function code into each Lambda from the console.

---

## Before you start

Gather these three values from your Amazon Connect instance — you'll type them
into the stack parameters:

- **Connect instance ID** — the GUID only, *not* the full ARN.
  Find it in the Connect console under your instance's **Overview**, or take the
  part after `instance/` in the instance ARN
  (`arn:aws:connect:…:instance/<THIS-PART>`).
- **Task contact flow ID** — the task flow that scheduled callback tasks will run.
  Open the flow in the Connect flow designer; the ID is the last path segment of
  `…/contact-flow/<THIS-PART>`, or use **Show additional flow information**.
- **Queue ID** *(optional)* — a fallback queue used only if your flow doesn't
  pass a `queueId`. Leave blank to require the flow to supply it.

Also have your two source files ready to paste later:
- the slot-suggester code (from `callback_scheduler.py`)
- the slot-booking code (from `callback_booking.py`)

You need an IAM user or role with permission to create CloudFormation stacks,
IAM roles, Lambda functions, and DynamoDB tables.

---

## Step 1 — Open CloudFormation and start a new stack

1. Sign in to the AWS Console and make sure the **Region selector** (top right)
   is set to the same region as your Connect instance. Everything must live in
   that region.
2. Search for and open **CloudFormation**.
3. Choose **Create stack → With new resources (standard)**.

---

## Step 2 — Upload the template

1. Under **Prepare template**, select **Choose an existing template**.
2. Under **Specify template**, select **Upload a template file**.
3. Click **Choose file** and pick `callback-system.yaml`.
4. Click **Next**.

---

## Step 3 — Fill in the stack details

1. **Stack name** — enter something like `callbacks`.
2. Fill in the **Parameters**:
   - **ConnectInstanceId** — your instance GUID (required).
   - **TaskContactFlowId** — your task contact flow ID (required).
   - **ConnectQueueId** — fallback queue ID, or leave blank.
   - The remaining parameters have sensible defaults; adjust if needed:
     - **BusinessTimezone** (default `America/New_York`)
     - **SlotMinutes** (default `15`), **SlotCapacity** (default `10`)
     - **LeadMinutes** (default `5`)
     - **FallbackStartHour** (default `9`), **FallbackEndHour** (default `20`)
     - **EndBufferMinutes** (default `15`)
     - **SlotsTableName** (default `callback_slots`),
       **CallbacksTableName** (default `callbacks_records`)
     - **LambdaRuntime** (default `python3.14`)
3. Click **Next**.

---

## Step 4 — Configure stack options

1. You can leave everything on this page at its defaults. Add tags if your
   organization requires them.
2. Click **Next**.

---

## Step 5 — Review and create

1. Review the summary.
2. At the bottom, find **Capabilities** and check the box:
   **"I acknowledge that AWS CloudFormation might create IAM resources."**
   (The stack creates the two Lambda execution roles.)
3. Click **Submit** (or **Create stack**).

The stack status moves to `CREATE_IN_PROGRESS`. Wait for **`CREATE_COMPLETE`**
(usually under a minute or two). Use the refresh icon on the **Events** tab to
watch progress. If it fails, the Events tab shows the reason — see
**Troubleshooting** below.

---

## Step 6 — Note the outputs

Open the stack's **Outputs** tab. You'll see:

- `SlotsTableName`, `CallbacksTableName` — the two DynamoDB tables.
- `SuggesterFunctionArn`, `BookingFunctionArn` — the two Lambda ARNs
  (you'll need these when adding the functions to Connect).

At this point all infrastructure exists, but both Lambdas still contain
placeholder code. Replace it next.

---

## Step 7 — Paste the real code into each Lambda

The console code editor has no size limit (the 4096-character limit only applies
to code embedded in a CloudFormation template), so you can paste the full source
directly. The runtime already includes `boto3`, so there are no dependencies to
package.

Do this once for each function.

### Slot suggester

1. Open the **Lambda** console → **Functions** → **callback-slot-suggester**.
2. On the **Code** tab, open **index.py** in the editor.
3. Select all existing (placeholder) code and delete it.
4. Paste the full slot-suggester source (`callback_scheduler.py`).
5. Click **Deploy** (Ctrl/Cmd + S). Wait for "Changes deployed."

### Slot booking

1. Open **Lambda** → **Functions** → **callback-slot-booking**.
2. On the **Code** tab, open **index.py**.
3. Delete the placeholder code and paste the full slot-booking source
   (`callback_booking.py`).
4. Click **Deploy**.

> The handler is already set to `index.lambda_handler`, so keep the file named
> **index.py**. Don't rename it.

### Optional: quick smoke test

For each function, open the **Test** tab, create a test event with `{}` as the
body, and run it. A placeholder-free function that can't find real contact data
will return a controlled error like `"Missing queueId"` or
`"Missing caller phone from CustomerEndpoint"` — that's expected for an empty
event and confirms your code is running, not the placeholder.

---

## Step 8 — Make the functions available to Amazon Connect

Before a contact flow can call the Lambdas, the Connect instance must be allowed
to use them. (The template already added the resource permission; this step
registers them in the Connect instance.)

1. Open the **Amazon Connect** console → **Instances** → select your instance.
2. In the left menu, choose **Flows**.
3. Scroll to the **AWS Lambda** section.
4. Under **Function**, select **callback-slot-suggester** and click **Add Lambda
   Function**.
5. Repeat for **callback-slot-booking**.

---

## Step 9 — Call the functions from a contact flow

In your contact flow (the one whose ID you supplied):

1. Add an **Invoke AWS Lambda function** block where you want to suggest slots,
   and select **callback-slot-suggester**.
2. Pass any contact attributes you want to override (e.g. `maxSuggestions`,
   `offsetDays`, `timeRanges`). The function also reads the contact's queue and
   caller number automatically.
3. Later in the flow, add another **Invoke AWS Lambda function** block for
   **callback-slot-booking**, passing the caller's chosen slot as `chosenIso`.
4. Save and publish the flow, then place a test call.

Booked callbacks appear as rows in the `callbacks_records` table, and each slot's
running count is tracked in `callback_slots`.

---

## Updating later

- **To change the code:** repeat Step 7 (paste + Deploy). No stack update needed.
- **To change a setting** (slot length, capacity, timezone, table names, etc.):
  open the CloudFormation stack → **Update** → **Use existing template** →
  change the parameter → run the update. Changing a table *name* replaces the
  table, so avoid that on anything with live data.

---

## Cleanup

To remove everything: open the **CloudFormation** console, select the stack, and
choose **Delete**. This removes both Lambdas, both IAM roles, the permissions,
and both DynamoDB tables **(including all booking data)**. If you added the
functions to Connect in Step 8, remove them there first (Connect → your instance
→ Flows → AWS Lambda), otherwise the Connect association can block deletion.

---

## Troubleshooting

- **Stack fails with a capabilities error** — you didn't check the IAM
  acknowledgment box in Step 5. Delete the failed stack and recreate.
- **`ROLLBACK_COMPLETE` on first create** — read the **Events** tab for the first
  red failure; it's usually a duplicate DynamoDB table name (a table named
  `callback_slots` or `callbacks_records` already exists) or an invalid
  `ConnectInstanceId`. Fix the parameter and recreate.
- **Function returns `ZoneInfoNotFoundError`** — the selected runtime lacks the
  timezone database. On `python3.12`+ this shouldn't happen; if it does,
  either switch the `LambdaRuntime` parameter or bundle the `tzdata` package with
  your code.
- **Connect can't see the function** — make sure you completed Step 8, and that
  the function and the Connect instance are in the **same region**.
- **`AccessDenied` calling Connect or DynamoDB at runtime** — confirm
  `ConnectInstanceId` was entered as the plain GUID (not the full ARN); the IAM
  policies are scoped to `instance/<that id>/*`.
