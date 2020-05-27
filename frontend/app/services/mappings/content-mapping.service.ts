import { Injectable } from "@angular/core";
import { DataService } from "../data.service";
import { CommonService } from "@geonature_common/service/common.service";
import { ModuleConfig } from "../../module.config";

@Injectable()
export class ContentMappingService {
  public userContentMappings;
  public newMapping: boolean = false;
  public id_mapping;
  public displayMapped: boolean;

  constructor(
    private _ds: DataService,
    private _commonService: CommonService
  ) {
    this.displayMapped = ModuleConfig.DISPLAY_MAPPED_VALUES;
  }

  getMappingNamesList(newContentId?, formControl?) {
    // get list of existing content mapping in the select
    this._ds.getMappings("content").subscribe(
      result => {
        this.userContentMappings = result
        if (newContentId) {
          const newMapping = result.find(el => {
            return el.id_mapping == newContentId
          })
          formControl.setValue(newMapping)
        }

      },
      error => {
        console.log(error);
        if (error.statusText === "Unknown Error") {
          // show error message if no connexion
          this._commonService.regularToaster(
            "error",
            "ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)"
          );
        } else {
          console.log(error);
          this._commonService.regularToaster("error", error.error);
        }
      }
    );
  }

  createMapping(mappingForm) {
    mappingForm.reset();
    this.newMapping = true;
    this.displayMapped = true;
  }

  cancelMapping(mappingForm) {
    this.newMapping = false;
    mappingForm.controls["mappingName"].setValue("");
  }

}
