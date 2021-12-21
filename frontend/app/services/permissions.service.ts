import { Injectable } from "@angular/core";
import { map } from "rxjs/operators";

import { DataService } from "./data.service";

type Authorization = {
  is_authorized: boolean;
};

@Injectable()
export class PermissionsService {
  constructor(private _ds: DataService) {}

  canUserUpdate(importId: number) {
    return this._ds.canUpdateImport(importId).pipe(
      map((res: Authorization) => {
        return res.is_authorized;
      })
    );
  }
}
