-- Records the resolved final artifact path per attempt so reveal and
-- display views can use the actual published location.
ALTER TABLE attempts ADD COLUMN final_path TEXT;
